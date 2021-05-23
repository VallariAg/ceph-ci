// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_CACHEREQUEST_H
#define RGW_CACHEREQUEST_H

#include <fcntl.h>
#include <stdlib.h>
#include <aio.h>

#include "include/rados/librados.hpp"
#include "include/Context.h"

#include "rgw_aio.h"
#include "rgw_cache.h"


class D3nCacheRequest {
  public:
    std::mutex lock;
    int sequence;
    buffer::list* pbl;
    std::string oid;
    off_t ofs;
    off_t len;
    std::string key;
    off_t read_ofs;
    rgw::AioResult* r = nullptr;
    rgw::Aio* aio = nullptr;
    D3nCacheRequest() : sequence(0), pbl(nullptr), ofs(0), len(0), read_ofs(0) {};
    virtual ~D3nCacheRequest() {};
    virtual void d3n_libaio_release()=0;
    virtual void d3n_libaio_cancel_io()=0;
    virtual int d3n_libaio_status()=0;
    virtual void d3n_libaio_finish()=0;
};

struct D3nL1CacheRequest : public D3nCacheRequest {
  using sigval_cb = void (*) (sigval_t);
  int stat;
  int ret;
  struct aiocb* paiocb;
  static std::mutex d3n_libaio_cb_lock;

  D3nL1CacheRequest() :  D3nCacheRequest(), stat(-1), paiocb(nullptr) {}
  ~D3nL1CacheRequest() {
    const std::lock_guard<std::mutex> l(lock);
    if (paiocb != nullptr) {
      if (paiocb->aio_buf != nullptr) {
        free((void*)paiocb->aio_buf);
        paiocb->aio_buf = nullptr;
      }
      ::close(paiocb->aio_fildes);
      delete(paiocb);
    }

    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "(): Read From Cache, comlete" << dendl;
  }

  int d3n_execute_io_op(std::string obj_key, bufferlist* bl, int read_len, int ofs, int read_ofs, std::string& cache_location,
                    sigval_cb f, rgw::Aio* aio, rgw::AioResult* r) {
    std::string location = cache_location + "/" + obj_key;
    lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): Read From Cache, location='" << location << "', ofs=" << ofs << ", read_ofs=" << read_ofs << " read_len=" << read_len << dendl;
    int rfd;
    if ((rfd = ::open(location.c_str(), O_RDONLY)) == -1) {
      lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: Error: " << __func__ << "():  ::open(" << location << ") errno=" << errno << dendl;
      return -errno;
    }
    if((ret = posix_fadvise(rfd, 0, 0, g_conf()->rgw_d3n_l1_fadvise)) != 0) {
      lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: Warning: " << __func__ << "()  posix_fadvise( , , , "  << g_conf()->rgw_d3n_l1_fadvise << ") ret=" << ret << dendl;
    }
    if ((read_ofs > 0) && (::lseek(rfd, read_ofs, SEEK_SET) != read_ofs)) {
      lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: Error: " << __func__ << "()  ::lseek(" << location << ", read_ofs=" << read_ofs << ") errno=" << errno << dendl;
      return -errno;
    }
    char* io_buf = (char*)malloc(read_len);
    if (io_buf == NULL) {
      lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: Error: " << __func__ << "()  malloc(" << read_len << ") errno=" << errno << dendl;
      return -errno;
    }
    ssize_t nbytes;
    if ((nbytes = ::read(rfd, io_buf, read_len)) == -1) {
      lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: Error: " << __func__ << "()  ::read(" << location << ", read_ofs=" << read_ofs << ", read_len=" << read_len << ") errno=" << errno << dendl;
      free(io_buf);
      return -errno;
    }
    if (nbytes != read_len) {
      lsubdout(g_ceph_context, rgw, 0) << "D3nDataCache: Error: " << __func__ << "()  ::read(" << location << ", read_ofs=" << read_ofs << ", read_len=" << read_len << ") read_len!=nbytes = " << nbytes << dendl;
      free(io_buf);
      return -EIO;
    }
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "(): Read From Cache, nbytes=" << nbytes << dendl;
    bl->append(io_buf, nbytes);
    r->result = 0;
    aio->put(*(r));
    ::close(rfd);
    free(io_buf);
    return 0;
  }

  int d3n_prepare_libaio_op(std::string obj_key, bufferlist* bl, int read_len, int ofs, int read_ofs, std::string& cache_location,
                        sigval_cb cbf, rgw::Aio* aio, rgw::AioResult* r) {
    std::string location = cache_location + "/" + obj_key;
    lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): Read From Cache, location='" << location << "', ofs=" << ofs << ", read_ofs=" << read_ofs << " read_len=" << read_len << dendl;
    this->r = r;
    this->aio = aio;
    this->pbl = bl;
    this->ofs = ofs;
    this->key = obj_key;
    this->len = read_len;
    this->stat = EINPROGRESS;
    struct aiocb* cb = new struct aiocb;
    memset(cb, 0, sizeof(aiocb));
    cb->aio_fildes = ::open(location.c_str(), O_RDONLY);
    if (cb->aio_fildes < 0) {
      lsubdout(g_ceph_context, rgw, 0) << "Error: " << __func__ << " ::open(" << cache_location << ")" << dendl;
      return -errno;
    }
    if (g_conf()->rgw_d3n_l1_fadvise != 0)
      posix_fadvise(cb->aio_fildes, 0, 0, g_conf()->rgw_d3n_l1_fadvise);

    cb->aio_buf = (volatile void*)malloc(read_len);
    cb->aio_nbytes = read_len;
    cb->aio_offset = read_ofs;
    cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    cb->aio_sigevent.sigev_notify_function = cbf;
    cb->aio_sigevent.sigev_notify_attributes = NULL;

    cb->aio_sigevent.sigev_value.sival_ptr = this;
    this->paiocb = cb;
    return 0;
  }

  void d3n_libaio_release () {}

  void d3n_libaio_cancel_io() {
    const std::lock_guard<std::mutex> l(lock);
    stat = ECANCELED;
  }

  int d3n_libaio_status() {
    const std::lock_guard<std::mutex> l(lock);
    lsubdout(g_ceph_context, rgw_datacache, 30) << "D3nDataCache: " << __func__ << "(): key=" << key << ", stat=" << stat << dendl;
    if (stat != EINPROGRESS) {
      if (stat == ECANCELED) {
        lsubdout(g_ceph_context, rgw, 2) << "D3nDataCache: " << __func__ << "(): stat == ECANCELED" << dendl;
        return ECANCELED;
      }
    }
    stat = aio_error(paiocb);
    return stat;
  }

  void d3n_libaio_finish() {
    const std::lock_guard<std::mutex> l(lock);
    lsubdout(g_ceph_context, rgw_datacache, 20) << "D3nDataCache: " << __func__ << "(): Read From Cache, libaio callback - returning data: key=" << key << ", aio_nbytes=" << paiocb->aio_nbytes << dendl;
    pbl->append((char*)paiocb->aio_buf, paiocb->aio_nbytes);
  }
};

#endif
