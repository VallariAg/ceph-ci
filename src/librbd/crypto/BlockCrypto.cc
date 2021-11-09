// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/BlockCrypto.h"
#include "include/byteorder.h"
#include "include/ceph_assert.h"

#include <stdlib.h>

namespace librbd {
namespace crypto {

template <typename T>
BlockCrypto<T>::BlockCrypto(CephContext* cct, DataCryptor<T>* data_cryptor,
                            uint64_t block_size, uint64_t data_offset)
     : m_cct(cct), m_data_cryptor(data_cryptor), m_block_size(block_size),
       m_data_offset(data_offset), m_iv_size(data_cryptor->get_iv_size()) {
  ceph_assert(isp2(block_size));
  ceph_assert((block_size % data_cryptor->get_block_size()) == 0);
  ceph_assert((block_size % 512) == 0);
}

template <typename T>
BlockCrypto<T>::~BlockCrypto() {
  if (m_data_cryptor != nullptr) {
    delete m_data_cryptor;
    m_data_cryptor = nullptr;
  }
}

template <typename T>
int BlockCrypto<T>::crypt(ceph::bufferlist* data, uint64_t image_offset,
                           CipherMode mode) {
  if (image_offset % m_block_size != 0) {
    lderr(m_cct) << "image offset: " << image_offset
                 << " not aligned to block size: " << m_block_size << dendl;
    return -EINVAL;
  }
  if (data->length() % m_block_size != 0) {
    lderr(m_cct) << "data length: " << data->length()
                 << " not aligned to block size: " << m_block_size << dendl;
    return -EINVAL;
  }

  unsigned char* iv = (unsigned char*)alloca(m_iv_size);
  memset(iv, 0, m_iv_size);

  bufferlist src = *data;
  data->clear();

  auto ctx = m_data_cryptor->get_context(mode);
  if (ctx == nullptr) {
    lderr(m_cct) << "unable to get crypt context" << dendl;
    return -EIO;
  }
  auto appender = data->get_contiguous_appender(src.length());
  unsigned char* leftover_block = (unsigned char*)alloca(m_block_size);
  uint32_t leftover_size = 0;
  for (auto buf = src.buffers().begin(); buf != src.buffers().end(); ++buf) {
    auto in_buf_ptr = reinterpret_cast<const unsigned char*>(buf->c_str());
    auto remaining_buf_bytes = buf->length();
    while (remaining_buf_bytes > 0) {
      if (leftover_size > 0 || remaining_buf_bytes < m_block_size) {
        auto copy_size = std::min(
                (uint32_t)m_block_size - leftover_size, remaining_buf_bytes);
        memcpy(leftover_block + leftover_size, in_buf_ptr, copy_size);
        in_buf_ptr += copy_size;
        leftover_size += copy_size;
        remaining_buf_bytes -= copy_size;
      }

      const unsigned char* crypto_in_ptr = nullptr;
      if (leftover_size == 0) {
        crypto_in_ptr = in_buf_ptr;
        in_buf_ptr += m_block_size;
        remaining_buf_bytes -= m_block_size;
      } else if (leftover_size == m_block_size) {
        crypto_in_ptr = leftover_block;
        leftover_size = 0;
      } else {
        continue;
      }

      unsigned char* out_buf_ptr = reinterpret_cast<unsigned char*>(
              appender.get_pos_add(m_block_size));

      if (mode == CIPHER_MODE_DEC &&
        mem_is_zero(reinterpret_cast<const char*>(crypto_in_ptr),
                    m_block_size)) {
        // input is already plaintext (zeros), so don't decrypt
        memset(out_buf_ptr, 0, m_block_size);
      } else {
        uint64_t sector_number = image_offset / 512;
        auto block_offset_le = ceph_le64(sector_number);
        memcpy(iv, &block_offset_le, sizeof(block_offset_le));
        auto r = m_data_cryptor->init_context(ctx, iv, m_iv_size);
        if (r != 0) {
          lderr(m_cct) << "unable to init cipher's IV" << dendl;
          return r;
        }

        int crypto_output_length = m_data_cryptor->update_context(
                ctx, crypto_in_ptr, out_buf_ptr, m_block_size);
        if (crypto_output_length < 0) {
          lderr(m_cct) << "crypt update failed" << dendl;
          return crypto_output_length;
        }

        ceph_assert(crypto_output_length == static_cast<int>(m_block_size));
      }

      image_offset += m_block_size;
    }
  }

  m_data_cryptor->return_context(ctx, mode);

  return 0;
}

template <typename T>
int BlockCrypto<T>::encrypt(ceph::bufferlist* data, uint64_t image_offset) {
  return crypt(data, image_offset, CipherMode::CIPHER_MODE_ENC);
}

template <typename T>
int BlockCrypto<T>::decrypt(ceph::bufferlist* data, uint64_t image_offset) {
  return crypt(data, image_offset, CipherMode::CIPHER_MODE_DEC);
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::BlockCrypto<EVP_CIPHER_CTX>;
