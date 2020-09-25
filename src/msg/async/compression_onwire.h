// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2009 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */


#ifndef CEPH_COMPRESSION_ONWIRE_H
#define CEPH_COMPRESSION_ONWIRE_H

#include "compressor/Compressor.h"
#include "include/buffer.h"

namespace ceph {
    namespace compression {
        namespace onwire {
            class Handler {
            public:
                Handler(CephContext* const cct, CompressorRef compressor) 
                    : m_cct(cct), m_compressor(compressor) {};
                ~Handler() {m_compressor = nullptr;};

            protected:
                CephContext* const m_cct;
                CompressorRef m_compressor;
            };

            class RxHandler : public Handler {
            public:
                RxHandler(CephContext* const cct, CompressorRef compressor) 
                    : Handler(cct, compressor) {}
                ~RxHandler() {};
                bool decompress(const ceph::bufferlist &input, ceph::bufferlist &out);
            };

            class TxHandler : public Handler {
            public:
                TxHandler(CephContext* const cct, CompressorRef compressor, int mode, std::uint64_t min_size)
                    : Handler(cct, compressor), m_min_size(min_size) {
                        m_mode = static_cast<Compressor::CompressionMode>(mode);
                    } 
                ~TxHandler() {}

                void reset_handler(int num_segments, uint64_t size) {
                    m_init_onwire_size = size;
                    m_compress_potential = size;
                    m_onwire_size = 0;
                }

                void final();

                bool compress(const ceph::bufferlist &input, ceph::bufferlist &out);

                double get_ratio() const {
                    return get_initial_size() / (double) get_final_size();
                }

                uint64_t get_initial_size() const {
                    return m_init_onwire_size;
                } 

                uint64_t get_final_size() const {
                    return m_onwire_size;
                }

            private:
                uint64_t m_min_size; 
                Compressor::CompressionMode m_mode;

                uint64_t m_init_onwire_size;
                uint64_t m_onwire_size;
                uint64_t m_compress_potential;
            };

            struct rxtx_t {
                std::unique_ptr<RxHandler> rx;
                std::unique_ptr<TxHandler> tx;

                static rxtx_t create_handler_pair(
                    CephContext* ctx,
                    const CompConnectionMeta& comp_meta,
                    std::uint64_t compress_min_size);
            };
        }
    }
}

#endif // CEPH_COMPRESSION_ONWIRE_H
