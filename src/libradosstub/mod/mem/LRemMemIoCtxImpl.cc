// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemMemIoCtxImpl.h"
#include "LRemMemRadosClient.h"
#include "common/Clock.h"
#include "include/err.h"
#include <functional>
#include <boost/algorithm/string/predicate.hpp>
#include <errno.h>
#include <include/compat.h>

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "LRemMemIoCtxImpl: " << this << " " << __func__ \
                           << ": " << oid << " "

using namespace std;

static void to_vector(const interval_set<uint64_t> &set,
                      std::vector<std::pair<uint64_t, uint64_t> > *vec) {
  vec->clear();
  for (interval_set<uint64_t>::const_iterator it = set.begin();
      it != set.end(); ++it) {
    vec->push_back(*it);
  }
}

// see PrimaryLogPG::finish_extent_cmp()
static int cmpext_compare(const bufferlist &bl, const bufferlist &read_bl) {
  for (uint64_t idx = 0; idx < bl.length(); ++idx) {
    char read_byte = (idx < read_bl.length() ? read_bl[idx] : 0);
    if (bl[idx] != read_byte) {
      return -MAX_ERRNO - idx;
    }
  }
  return 0;
}

namespace librados {

LRemMemIoCtxImpl::LRemMemIoCtxImpl() {
}

LRemMemIoCtxImpl::LRemMemIoCtxImpl(const LRemMemIoCtxImpl& rhs)
    : LRemIoCtxImpl(rhs), m_client(rhs.m_client), m_pool(rhs.m_pool) {
  m_pool->get();
}

LRemMemIoCtxImpl::LRemMemIoCtxImpl(LRemMemRadosClient *client, int64_t pool_id,
                                   const std::string& pool_name,
                                   LRemMemCluster::Pool *pool)
    : LRemIoCtxImpl(client, pool_id, pool_name), m_client(client),
      m_pool(pool) {
  m_pool->get();
}

LRemMemIoCtxImpl::~LRemMemIoCtxImpl() {
  m_pool->put();
}

LRemIoCtxImpl *LRemMemIoCtxImpl::clone() {
  return new LRemMemIoCtxImpl(*this);
}

int LRemMemIoCtxImpl::aio_append(const std::string& oid, AioCompletionImpl *c,
                                 const bufferlist& bl, size_t len) {
  bufferlist newbl;
  newbl.substr_of(bl, 0, len);
  auto trans = init_transaction(oid);
  m_client->add_aio_operation(oid, true,
                              std::bind(&LRemMemIoCtxImpl::append, this, trans,
                                        newbl,
					get_snap_context()),
                              c);
  return 0;
}

int LRemMemIoCtxImpl::aio_remove(const std::string& oid, AioCompletionImpl *c, int flags) {
  auto trans = init_transaction(oid);
  m_client->add_aio_operation(oid, true,
                              std::bind(&LRemMemIoCtxImpl::remove, this, trans,
					get_snap_context()),
                              c);
  return 0;
}

int LRemMemIoCtxImpl::append(LRemTransactionStateRef& trans, const bufferlist &bl,
                             const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();
  ldout(cct, 20) << "length=" << bl.length() << ", snapc=" << snapc << dendl;

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  file = get_file_safe(trans, true, CEPH_NOSNAP, snapc, &epoch);
  epoch = ++m_pool->epoch;

  std::unique_lock l{file->lock};
  auto off = file->data.length();
  ensure_minimum_length(off + bl.length(), &file->data);
  file->data.begin(off).copy_in(bl.length(), bl);
  file->epoch = epoch;

  return 0;
}

int LRemMemIoCtxImpl::assert_exists(LRemTransactionStateRef& trans, uint64_t snap_id) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::shared_lock l{m_pool->file_lock};
  LRemMemCluster::SharedFile file = get_file(trans, false, snap_id, {});
  if (file == NULL) {
    return -ENOENT;
  }
  return 0;
}

int LRemMemIoCtxImpl::assert_version(LRemTransactionStateRef& trans, uint64_t ver) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::shared_lock l{m_pool->file_lock};
  LRemMemCluster::SharedFile file = get_file(trans, false, CEPH_NOSNAP, {});
  if (file == NULL || !file->exists) {
    return -ENOENT;
  }
  if (ver < file->objver) {
    return -ERANGE;
  }
  if (ver > file->objver) {
    return -EOVERFLOW;
  }

  return 0;
}

int LRemMemIoCtxImpl::create(LRemTransactionStateRef& trans, bool exclusive,
                             const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto& oid = trans->oid();

  auto cct = m_client->cct();
  ldout(cct, 20) << "snapc=" << snapc << dendl;

  std::unique_lock l{m_pool->file_lock};
  LRemMemCluster::SharedFile file = get_file(trans, false, CEPH_NOSNAP, {});
  bool exists = (file != NULL && file->exists);
  if (exists) {
    return (exclusive ? -EEXIST : 0);
  }

  auto new_file = get_file(trans, true, CEPH_NOSNAP, snapc);
  new_file->epoch = ++m_pool->epoch;
  return 0;
}

int LRemMemIoCtxImpl::list_snaps(LRemTransactionStateRef& trans, snap_set_t *out_snaps) {
  auto& oid = trans->oid();
  auto cct = m_client->cct();
  ldout(cct, 20) << dendl;

  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  out_snaps->seq = 0;
  out_snaps->clones.clear();

  std::shared_lock l{m_pool->file_lock};
  LRemMemCluster::Files::iterator it = m_pool->files.find(trans->locator);
  if (it == m_pool->files.end()) {
    return -ENOENT;
  }

  bool include_head = false;
  LRemMemCluster::FileSnapshots &file_snaps = it->second;
  for (LRemMemCluster::FileSnapshots::iterator s_it = file_snaps.begin();
       s_it != file_snaps.end(); ++s_it) {
    LRemMemCluster::File &file = *s_it->get();

    if (file_snaps.size() > 1) {
      out_snaps->seq = file.snap_id;
      LRemMemCluster::FileSnapshots::iterator next_it(s_it);
      ++next_it;
      if (next_it == file_snaps.end()) {
        include_head = true;
        break;
      }

      ++out_snaps->seq;
      if (!file.exists) {
        continue;
      }

      // update the overlap with the next version's overlap metadata
      LRemMemCluster::File &next_file = *next_it->get();
      interval_set<uint64_t> overlap;
      if (next_file.exists) {
        overlap = next_file.snap_overlap;
      }

      clone_info_t clone;
      clone.cloneid = file.snap_id;
      clone.snaps = file.snaps;
      to_vector(overlap, &clone.overlap);
      clone.size = file.data.length();
      out_snaps->clones.push_back(clone);
    }
  }

  if ((file_snaps.size() == 1 && file_snaps.back()->data.length() > 0) ||
      include_head)
  {
    // Include the SNAP_HEAD
    LRemMemCluster::File &file = *file_snaps.back();
    if (file.exists) {
      std::shared_lock l2{file.lock};
      if (out_snaps->seq == 0 && !include_head) {
        out_snaps->seq = file.snap_id;
      }
      clone_info_t head_clone;
      head_clone.cloneid = librados::SNAP_HEAD;
      head_clone.size = file.data.length();
      out_snaps->clones.push_back(head_clone);
    }
  }

  ldout(cct, 20) << "seq=" << out_snaps->seq << ", "
                 << "clones=[";
  bool first_clone = true;
  for (auto& clone : out_snaps->clones) {
    *_dout << "{"
           << "cloneid=" << clone.cloneid << ", "
           << "snaps=" << clone.snaps << ", "
           << "overlap=" << clone.overlap << ", "
           << "size=" << clone.size << "}";
    if (!first_clone) {
      *_dout << ", ";
    } else {
      first_clone = false;
    }
  }
  *_dout << "]" << dendl;
  return 0;

}

int LRemMemIoCtxImpl::omap_get_vals2(LRemTransactionStateRef& trans,
                                    const std::string& start_after,
                                    const std::string &filter_prefix,
                                    uint64_t max_return,
                                    std::map<std::string, bufferlist> *out_vals,
                                    bool *pmore) {
  if (out_vals == NULL) {
    return -EINVAL;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  LRemMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(trans, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  out_vals->clear();

  std::shared_lock l{file->lock};
  LRemMemCluster::FileOMaps::iterator o_it = m_pool->file_omaps.find(trans->locator);
  if (o_it == m_pool->file_omaps.end()) {
    if (pmore) {
      *pmore = false;
    }
    return 0;
  }

  auto& omap = o_it->second.data;
  auto it = omap.begin();
  if (!start_after.empty()) {
    it = omap.upper_bound(start_after);
  }

  while (it != omap.end() && max_return > 0) {
    if (filter_prefix.empty() ||
        boost::algorithm::starts_with(it->first, filter_prefix)) {
      (*out_vals)[it->first] = it->second;
      --max_return;
    }
    ++it;
  }
  if (pmore) {
    *pmore = (it != omap.end());
  }
  return 0;
}

int LRemMemIoCtxImpl::omap_get_vals(LRemTransactionStateRef& trans,
                                    const std::string& start_after,
                                    const std::string &filter_prefix,
                                    uint64_t max_return,
                                    std::map<std::string, bufferlist> *out_vals) {
  return omap_get_vals2(trans, start_after, filter_prefix, max_return, out_vals, nullptr);
}

int LRemMemIoCtxImpl::omap_get_vals_by_keys(LRemTransactionStateRef& trans,
                                            const std::set<std::string>& keys,
                                            std::map<std::string, bufferlist> *vals) {
  if (vals == NULL) {
    return -EINVAL;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  LRemMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(trans, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  vals->clear();

  std::shared_lock l{file->lock};
  LRemMemCluster::FileOMaps::iterator o_it = m_pool->file_omaps.find(trans->locator);
  if (o_it == m_pool->file_omaps.end()) {
    return 0;
  }
  auto& omap = o_it->second.data;
  for (const auto& key : keys) {
    auto viter = omap.find(key);
    if (viter != omap.end()) {
      (*vals)[key] = viter->second;
    }
  }

  return 0;
}

int LRemMemIoCtxImpl::omap_rm_keys(LRemTransactionStateRef& trans,
                                   const std::set<std::string>& keys) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  for (std::set<std::string>::iterator it = keys.begin();
       it != keys.end(); ++it) {
    m_pool->file_omaps[trans->locator].data.erase(*it);
  }
  file->epoch = epoch;
  return 0;
}

int LRemMemIoCtxImpl::omap_rm_range(LRemTransactionStateRef& trans,
                                    const string& key_begin,
                                    const string& key_end) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};

  auto& omap = m_pool->file_omaps[trans->locator].data;

  auto start = omap.lower_bound(key_begin);
  if (start == omap.end()) {
    return 0;
  }
  auto end = omap.lower_bound(key_end);

  omap.erase(start, end);

  file->epoch = epoch;

  return 0;
}

int LRemMemIoCtxImpl::omap_clear(LRemTransactionStateRef& trans) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  m_pool->file_omaps[trans->locator].data.clear();
  file->epoch = epoch;

  return 0;
}

int LRemMemIoCtxImpl::omap_set(LRemTransactionStateRef& trans,
                               const std::map<std::string, bufferlist> &map) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  for (std::map<std::string, bufferlist>::const_iterator it = map.begin();
      it != map.end(); ++it) {
    bufferlist bl;
    bl.append(it->second);
    m_pool->file_omaps[trans->locator].data[it->first] = bl;
  }
  file->epoch = epoch;

  return 0;
}

int LRemMemIoCtxImpl::omap_get_header(LRemTransactionStateRef& trans,
                                      bufferlist *bl) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();
  ldout(cct, 20) << ": <noargs>" << dendl;

  LRemMemCluster::SharedFile file;
  file = get_file_safe(trans, false, CEPH_NOSNAP, {});
  if (file == NULL) {
    return -ENOENT;
  }

  std::shared_lock l{file->lock};
  auto iter = m_pool->file_omaps.find(trans->locator);
  if (iter == m_pool->file_omaps.end()) {
    bl->clear();
  } else {
    *bl = iter->second.header;
  }

  return 0;
}

int LRemMemIoCtxImpl::omap_set_header(LRemTransactionStateRef& trans,
                                      const bufferlist& bl) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, get_snap_context());
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  m_pool->file_omaps[trans->locator].header = bl;
  file->epoch = epoch;

  return 0;
}

int LRemMemIoCtxImpl::read(LRemTransactionStateRef& trans, size_t len, uint64_t off,
                           bufferlist *bl, uint64_t snap_id,
                           uint64_t* objver) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  LRemMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(trans, false, snap_id, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{file->lock};
  if (len == 0) {
    len = file->data.length();
  }
  len = clip_io(off, len, file->data.length());
  if (bl != NULL && len > 0) {
    bufferlist bit;
    bit.substr_of(file->data, off, len);
    append_clone(bit, bl);
  }
  if (objver != nullptr) {
    *objver = file->objver;
  }
  return len;
}

int LRemMemIoCtxImpl::remove(LRemTransactionStateRef& trans, const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto& oid = trans->oid();
  auto cct = m_client->cct();
  ldout(cct, 20) << "snapc=" << snapc << dendl;

  std::unique_lock l{m_pool->file_lock};
  LRemMemCluster::SharedFile file = get_file(trans, false, CEPH_NOSNAP, snapc);
  if (file == NULL) {
    return -ENOENT;
  }
  file = get_file(trans, true, CEPH_NOSNAP, snapc);

  {
    std::unique_lock l2{file->lock};
    file->exists = false;
  }

  auto& locator = trans->locator;
  LRemMemCluster::Files::iterator it = m_pool->files.find(locator);
  ceph_assert(it != m_pool->files.end());

  if (*it->second.rbegin() == file) {
    LRemMemCluster::ObjectHandlers object_handlers;
    std::swap(object_handlers, m_pool->file_handlers[locator]);
    m_pool->file_handlers.erase(locator);

    for (auto object_handler : object_handlers) {
      object_handler->handle_removed(m_client);
    }
  }

  if (it->second.size() == 1) {
    m_pool->files.erase(it);
    m_pool->file_omaps.erase(locator);
  }
  ++m_pool->epoch;
  return 0;
}

int LRemMemIoCtxImpl::selfmanaged_snap_create(uint64_t *snapid) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  *snapid = ++m_pool->snap_id;
  m_pool->snap_seqs.insert(*snapid);
  ++m_pool->epoch;
  return 0;
}

int LRemMemIoCtxImpl::selfmanaged_snap_remove(uint64_t snapid) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};
  LRemMemCluster::SnapSeqs::iterator it =
    m_pool->snap_seqs.find(snapid);
  if (it == m_pool->snap_seqs.end()) {
    return -ENOENT;
  }

  // TODO clean up all file snapshots
  m_pool->snap_seqs.erase(it);
  ++m_pool->epoch;
  return 0;
}

int LRemMemIoCtxImpl::selfmanaged_snap_rollback(LRemTransactionStateRef& trans,
                                                uint64_t snapid) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  std::unique_lock l{m_pool->file_lock};

  LRemMemCluster::SharedFile file;
  LRemMemCluster::Files::iterator f_it = m_pool->files.find(trans->locator);
  if (f_it == m_pool->files.end()) {
    return 0;
  }

  LRemMemCluster::FileSnapshots &snaps = f_it->second;
  file = snaps.back();

  size_t versions = 0;
  for (LRemMemCluster::FileSnapshots::reverse_iterator it = snaps.rbegin();
      it != snaps.rend(); ++it) {
    LRemMemCluster::SharedFile file = *it;
    if (file->snap_id < get_snap_read()) {
      if (versions == 0) {
        // already at the snapshot version
        return 0;
      } else if (file->snap_id == CEPH_NOSNAP) {
        if (versions == 1) {
          // delete it current HEAD, next one is correct version
          snaps.erase(it.base());
        } else {
          // overwrite contents of current HEAD
          file = LRemMemCluster::SharedFile (new LRemMemCluster::File(**it));
          file->snap_id = CEPH_NOSNAP;
          *it = file;
        }
      } else {
        // create new head version
        file = LRemMemCluster::SharedFile (new LRemMemCluster::File(**it));
        file->snap_id = m_pool->snap_id;
        snaps.push_back(file);
      }
      return 0;
    }
    ++versions;
  }
  ++m_pool->epoch;
  return 0;
}

int LRemMemIoCtxImpl::set_alloc_hint(LRemTransactionStateRef& trans,
                                     uint64_t expected_object_size,
                                     uint64_t expected_write_size,
                                     uint32_t flags,
                                     const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  {
    std::unique_lock l{m_pool->file_lock};
    get_file(trans, true, CEPH_NOSNAP, snapc);
  }

  /* this one doesn't really do anything, so not updating pool/file epoch */

  return 0;
}

int LRemMemIoCtxImpl::sparse_read(LRemTransactionStateRef& trans, uint64_t off,
                                  uint64_t len,
                                  std::map<uint64_t,uint64_t> *m,
                                  bufferlist *data_bl, uint64_t snap_id,
                                  uint64_t truncate_size,
                                  uint32_t truncate_seq) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  // TODO verify correctness
  LRemMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(trans, false, snap_id, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{file->lock};
  len = clip_io(off, len, file->data.length());
  // TODO support sparse read
  if (m != NULL) {
    m->clear();
    if (len > 0) {
      (*m)[off] = len;
    }
  }
  if (data_bl != NULL && len > 0) {
    bufferlist bit;
    bit.substr_of(file->data, off, len);
    append_clone(bit, data_bl);
  }
  return len > 0 ? 1 : 0;
}

int LRemMemIoCtxImpl::stat2(LRemTransactionStateRef& trans, uint64_t *psize,
                            struct timespec *pts) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();
  ldout(cct, 20) << ": <noargs>" << dendl;

  LRemMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(trans, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }
  }

  std::shared_lock l{file->lock};
  if (psize != NULL) {
    *psize = file->data.length();
  }
  if (pts != NULL) {
    *pts = file->mtime;
  }
  return 0;
}

int LRemMemIoCtxImpl::mtime2(LRemTransactionStateRef& trans, const struct timespec& ts,
                             const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, snapc);
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  file->mtime = ts;
  file->epoch = epoch;

  return 0;
}

int LRemMemIoCtxImpl::truncate(LRemTransactionStateRef& trans, uint64_t size,
                               const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto& oid = trans->oid();
  auto cct = m_client->cct();
  ldout(cct, 20) << "size=" << size << ", snapc=" << snapc << dendl;

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, snapc);
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  bufferlist bl(size);

  interval_set<uint64_t> is;
  if (file->data.length() > size) {
    is.insert(size, file->data.length() - size);

    bl.substr_of(file->data, 0, size);
    file->data.swap(bl);
  } else if (file->data.length() != size) {
    if (size == 0) {
      bl.clear();
    } else {
      is.insert(0, size);

      bl.append_zero(size - file->data.length());
      file->data.append(bl);
    }
  }
  is.intersection_of(file->snap_overlap);
  file->snap_overlap.subtract(is);
  file->epoch = epoch;
  return 0;
}

int LRemMemIoCtxImpl::write(LRemTransactionStateRef& trans, bufferlist& bl, size_t len,
                            uint64_t off, const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto& oid = trans->oid();
  auto cct = m_client->cct();
  ldout(cct, 20) << "extent=" << off << "~" << len << ", snapc=" << snapc
                 << dendl;
  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, snapc);
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  if (len > 0) {
    interval_set<uint64_t> is;
    is.insert(off, len);
    is.intersection_of(file->snap_overlap);
    file->snap_overlap.subtract(is);
  }

  ensure_minimum_length(off + len, &file->data);
  file->data.begin(off).copy_in(len, bl);
  file->epoch = epoch;
  return 0;
}

int LRemMemIoCtxImpl::write_full(LRemTransactionStateRef& trans, bufferlist& bl,
                                 const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto& oid = trans->oid();
  auto cct = m_client->cct();
  ldout(cct, 20) << "length=" << bl.length() << ", snapc=" << snapc << dendl;
  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, snapc);
    if (file == NULL) {
      return -ENOENT;
    }
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  if (bl.length() > 0) {
    interval_set<uint64_t> is;
    is.insert(0, bl.length());
    is.intersection_of(file->snap_overlap);
    file->snap_overlap.subtract(is);
  }

  file->data.clear();
  ensure_minimum_length(bl.length(), &file->data);
  file->data.begin().copy_in(bl.length(), bl);
  file->epoch = epoch;
  return 0;
}

int LRemMemIoCtxImpl::writesame(LRemTransactionStateRef& trans, bufferlist& bl,
                                size_t len, uint64_t off,
                                const SnapContext &snapc) {
  if (get_snap_read() != CEPH_NOSNAP) {
    return -EROFS;
  } else if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  if (len == 0 || (len % bl.length())) {
    return -EINVAL;
  }

  uint64_t epoch;

  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, true, CEPH_NOSNAP, snapc);
    epoch = ++m_pool->epoch;
  }

  std::unique_lock l{file->lock};
  if (len > 0) {
    interval_set<uint64_t> is;
    is.insert(off, len);
    is.intersection_of(file->snap_overlap);
    file->snap_overlap.subtract(is);
  }

  ensure_minimum_length(off + len, &file->data);
  while (len > 0) {
    file->data.begin(off).copy_in(bl.length(), bl);
    off += bl.length();
    len -= bl.length();
  }

  file->epoch = epoch;

  return 0;
}

int LRemMemIoCtxImpl::cmpext(LRemTransactionStateRef& trans, uint64_t off,
                             bufferlist& cmp_bl, uint64_t snap_id) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  bufferlist read_bl;
  uint64_t len = cmp_bl.length();

  LRemMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(trans, false, snap_id, {});
    if (file == NULL) {
      return cmpext_compare(cmp_bl, read_bl);
    }
  }

  std::shared_lock l{file->lock};
  if (off >= file->data.length()) {
    len = 0;
  } else if (off + len > file->data.length()) {
    len = file->data.length() - off;
  }
  read_bl.substr_of(file->data, off, len);
  return cmpext_compare(cmp_bl, read_bl);
}

int LRemMemIoCtxImpl::cmpxattr_str(LRemTransactionStateRef& trans,
                                   const char *name, uint8_t op, const bufferlist& bl)
{
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  LRemMemCluster::SharedFile file;
  std::shared_lock l{m_pool->file_lock};
  LRemMemCluster::FileXAttrs::iterator it = m_pool->file_xattrs.find(trans->locator);
  if (it == m_pool->file_xattrs.end()) {
    return -ENODATA;
  }
  auto& attrset = it->second;

  auto iter = attrset.find(name);
  if (iter == attrset.end()) {
    return -ENODATA;
  }

  auto& attr_bl = iter->second;

  bool cmp;

  switch (op) {
    case CEPH_OSD_CMPXATTR_OP_EQ:
      cmp = (bl == attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_NE:
      cmp = (bl != attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_GT:
      cmp = (bl > attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_GTE:
      cmp = (bl >= attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_LT:
      cmp = (bl < attr_bl);
      break;
    case CEPH_OSD_CMPXATTR_OP_LTE:
      cmp = (bl <= attr_bl);
      break;
    default:
      return -EINVAL;
  }

  if (!cmp) {
    return -ECANCELED;
  }

  return 0;
}

int LRemMemIoCtxImpl::cmpxattr(LRemTransactionStateRef& trans,
                               const char *name, uint8_t op, uint64_t v)
{
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  LRemMemCluster::SharedFile file;
  std::shared_lock l{m_pool->file_lock};
  LRemMemCluster::FileXAttrs::iterator it = m_pool->file_xattrs.find(trans->locator);
  if (it == m_pool->file_xattrs.end()) {
    return -ENODATA;
  }
  auto& attrset = it->second;

  auto iter = attrset.find(name);
  if (iter == attrset.end()) {
    return -ENODATA;
  }

  auto& bl = iter->second;
  string s = bl.to_str();
  string err;

  auto& oid = trans->oid();
  auto cct = m_client->cct();
  ldout(cct, 20) << "cmpxattr name=" << name << " s=" << s << " v=" << v << dendl;

  uint64_t attr_val = (s.empty() ? 0 : static_cast<int64_t>(strict_strtoll(s, 10, &err)));
  if (!err.empty()) {
    return -EINVAL;
  }

  bool cmp;

  switch (op) {
    case CEPH_OSD_CMPXATTR_OP_EQ:
      cmp = (v == attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_NE:
      cmp = (v != attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_GT:
      cmp = (v > attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_GTE:
      cmp = (v >= attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_LT:
      cmp = (v < attr_val);
      break;
    case CEPH_OSD_CMPXATTR_OP_LTE:
      cmp = (v <= attr_val);
      break;
    default:
      return -EINVAL;
  }

  if (!cmp) {
    return -ECANCELED;
  }

  return 0;
}


int LRemMemIoCtxImpl::xattr_get(LRemTransactionStateRef& trans,
                                std::map<std::string, bufferlist>* attrset) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  int r = pool_op(trans, false, [&](LRemMemCluster::Pool *pool, bool write) {
    LRemMemCluster::FileXAttrs::iterator it = m_pool->file_xattrs.find(trans->locator);
    if (it == pool->file_xattrs.end()) {
      attrset->clear();
      return 0;
    }
    *attrset = it->second;

    return 0;
  });

  if (r < 0) {
    return r;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();
  ldout(cct, 20) << ": -> attrset=" << *attrset << dendl;

  return 0;
}

int LRemMemIoCtxImpl::setxattr(LRemTransactionStateRef& trans, const char *name,
                               bufferlist& bl) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();

  ldout(cct, 20) << ": -> name=" << name << " bl=" << bl << dendl;

  return pool_op(trans, true, [&](LRemMemCluster::Pool *pool, bool write) {
    pool->file_xattrs[trans->locator][name] = bl;
    return 0;
  });
}

int LRemMemIoCtxImpl::rmxattr(LRemTransactionStateRef& trans, const char *name) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto cct = m_client->cct();
  auto& oid = trans->oid();

  ldout(cct, 20) << ": -> name=" << name << dendl;

  return pool_op(trans, true, [&](LRemMemCluster::Pool *pool, bool write) {
    pool->file_xattrs[trans->locator].erase(name);
    return 0;
  });
}

int LRemMemIoCtxImpl::zero(LRemTransactionStateRef& trans, uint64_t off, uint64_t len,
                           const SnapContext &snapc) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  auto& oid = trans->oid();
  auto cct = m_client->cct();
  ldout(cct, 20) << "extent=" << off << "~" << len << ", snapc=" << snapc
                 << dendl;

  bool truncate_redirect = false;
  LRemMemCluster::SharedFile file;
  {
    std::unique_lock l{m_pool->file_lock};
    file = get_file(trans, false, CEPH_NOSNAP, snapc);
    if (!file) {
      return 0;
    }
    file = get_file(trans, true, CEPH_NOSNAP, snapc);

    std::shared_lock l2{file->lock};
    if (len > 0 && off + len >= file->data.length()) {
      // Zero -> Truncate logic embedded in OSD
      truncate_redirect = true;
    }
    file->epoch = ++m_pool->epoch;
  }
  if (truncate_redirect) {
    return truncate(trans, off, snapc);
  }

  bufferlist bl;
  bl.append_zero(len);
  return write(trans, bl, len, off, snapc);
}

int LRemMemIoCtxImpl::get_current_ver(LRemTransactionStateRef& trans, uint64_t *ver) {
  if (m_client->is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  LRemMemCluster::SharedFile file;
  {
    std::shared_lock l{m_pool->file_lock};
    file = get_file(trans, false, CEPH_NOSNAP, {});
    if (file == NULL) {
      return -ENOENT;
    }

    *ver = file->epoch;
  }

  return 0;
}

void LRemMemIoCtxImpl::append_clone(bufferlist& src, bufferlist* dest) {
  // deep-copy the src to ensure our memory-based mock RADOS data cannot
  // be modified by callers
  if (src.length() > 0) {
    bufferlist::iterator iter = src.begin();
    buffer::ptr ptr;
    iter.copy_deep(src.length(), ptr);
    dest->append(ptr);
  }
}

size_t LRemMemIoCtxImpl::clip_io(size_t off, size_t len, size_t bl_len) {
  if (off >= bl_len) {
    len = 0;
  } else if (off + len > bl_len) {
    len = bl_len - off;
  }
  return len;
}

void LRemMemIoCtxImpl::ensure_minimum_length(size_t len, bufferlist *bl) {
  if (len > bl->length()) {
    bufferptr ptr(buffer::create(len - bl->length()));
    ptr.zero();
    bl->append(ptr);
  }
}

LRemMemCluster::SharedFile LRemMemIoCtxImpl::get_file(
    LRemTransactionStateRef& trans, bool write, uint64_t snap_id,
    const SnapContext &snapc) {
  ceph_assert(ceph_mutex_is_locked(m_pool->file_lock) ||
	      ceph_mutex_is_wlocked(m_pool->file_lock));
  ceph_assert(!write || ceph_mutex_is_wlocked(m_pool->file_lock));

  LRemMemCluster::SharedFile file;
  LRemMemCluster::Files::iterator it = m_pool->files.find(trans->locator);
  if (it != m_pool->files.end()) {
    file = it->second.back();
  } else if (!write) {
    return LRemMemCluster::SharedFile();
  }

  if (write) {
    bool new_version = false;
    if (!file || !file->exists) {
      file = LRemMemCluster::SharedFile(new LRemMemCluster::File());
      new_version = true;
    } else {
      if (!snapc.snaps.empty() && file->snap_id < snapc.seq) {
        for (std::vector<snapid_t>::const_reverse_iterator seq_it =
            snapc.snaps.rbegin();
            seq_it != snapc.snaps.rend(); ++seq_it) {
          if (*seq_it > file->snap_id && *seq_it <= snapc.seq) {
            file->snaps.push_back(*seq_it);
          }
        }

        bufferlist prev_data = file->data;
        file = LRemMemCluster::SharedFile(
          new LRemMemCluster::File(*file));
        file->data.clear();
        append_clone(prev_data, &file->data);
        if (prev_data.length() > 0) {
          file->snap_overlap.insert(0, prev_data.length());
        }
        new_version = true;
      }
    }

    if (new_version) {
      file->snap_id = snapc.seq;
      file->mtime = real_clock::to_timespec(real_clock::now());
      m_pool->files[trans->locator].push_back(file);
    }

    file->objver++;
    return file;
  }

  if (snap_id == CEPH_NOSNAP) {
    if (!file->exists) {
      ceph_assert(it->second.size() > 1);
      return LRemMemCluster::SharedFile();
    }
    return file;
  }

  LRemMemCluster::FileSnapshots &snaps = it->second;
  for (LRemMemCluster::FileSnapshots::reverse_iterator it = snaps.rbegin();
      it != snaps.rend(); ++it) {
    LRemMemCluster::SharedFile file = *it;
    if (file->snap_id < snap_id) {
      if (!file->exists) {
        return LRemMemCluster::SharedFile();
      }
      return file;
    }
  }
  return LRemMemCluster::SharedFile();
}

LRemMemCluster::SharedFile LRemMemIoCtxImpl::get_file_safe(
    LRemTransactionStateRef& trans, bool write, uint64_t snap_id,
    const SnapContext &snapc,
    uint64_t *pepoch) {
  write |= trans->write;
  if (write) {
    std::unique_lock l{m_pool->file_lock};
    uint64_t epoch = ++m_pool->epoch;
    if (pepoch) {
      *pepoch = epoch;
    }
    return get_file(trans, true, snap_id, snapc);
  }

  std::shared_lock l{m_pool->file_lock};
  return get_file(trans, false, snap_id, snapc);
}

int LRemMemIoCtxImpl::pool_op(LRemTransactionStateRef& trans,
                              bool write,
                              PoolOperation op) {
  auto cct = m_client->cct();
  auto& oid = trans->oid();
  bool _write = (write | trans->write);
  ldout(cct, 20) << "pool_op() trans->write=" << trans->write << " write=" << write << " -> " << _write << dendl;

  if (_write) {
    std::unique_lock l{m_pool->file_lock};
    ++m_pool->epoch;
    return op(m_pool, true);
  }

  std::shared_lock l{m_pool->file_lock};
  return op(m_pool, false);
}

LRemTransactionStateRef LRemMemIoCtxImpl::init_transaction(const std::string& oid) {
  return make_op_transaction({get_namespace(), oid});
}

} // namespace librados
