// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/neorados/RADOS.hpp"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "common/hobject.h"
#include "librados/AioCompletionImpl.h"
#include "mon/error_code.h"
#include "osd/error_code.h"
#include "osd/osd_types.h"
#include "osdc/error_code.h"
#include "LibradosLRemStub.h"
#include "LRemClassHandler.h"
#include "LRemIoCtxImpl.h"
#include "LRemRadosClient.h"
#include "LRemTransaction.h"
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <functional>
#include <boost/system/system_error.hpp>

namespace bs = boost::system;
using namespace std::placeholders;
using namespace std::chrono_literals;

namespace neorados {
namespace detail {

struct Client {
  ceph::mutex mutex = ceph::make_mutex("NeoradosLRemStub::Client");

  librados::LRemRadosClient* lrem_rados_client;
  boost::asio::io_context& io_context;

  std::map<std::pair<int64_t, std::string>, librados::LRemIoCtxImpl*> io_ctxs;

  Client(librados::LRemRadosClient* lrem_rados_client)
    : lrem_rados_client(lrem_rados_client),
      io_context(lrem_rados_client->get_io_context()) {
  }

  ~Client() {
    for (auto& io_ctx : io_ctxs) {
      io_ctx.second->put();
    }
  }

  librados::LRemIoCtxImpl* get_io_ctx(const IOContext& ioc) {
    int64_t pool_id = ioc.pool();
    std::string ns = std::string{ioc.ns()};

    auto lock = std::scoped_lock{mutex};
    auto key = make_pair(pool_id, ns);
    auto it = io_ctxs.find(key);
    if (it != io_ctxs.end()) {
      return it->second;
    }

    std::list<std::pair<int64_t, std::string>> pools;
    int r = lrem_rados_client->pool_list(pools);
    if (r < 0) {
      return nullptr;
    }

    for (auto& pool : pools) {
      if (pool.first == pool_id) {
        auto io_ctx = lrem_rados_client->create_ioctx(pool_id, pool.second);
        io_ctx->set_namespace(ns);
        io_ctxs[key] = io_ctx;
        return io_ctx;
      }
    }
    return nullptr;
  }
};

} // namespace detail

namespace {

struct CompletionPayload {
  std::unique_ptr<Op::Completion> c;
};

void completion_callback_adapter(rados_completion_t c, void *arg) {
  auto impl = reinterpret_cast<librados::AioCompletionImpl *>(c);
  auto r = impl->get_return_value();
  impl->release();

  auto payload = reinterpret_cast<CompletionPayload*>(arg);
  payload->c->defer(std::move(payload->c),
                    (r < 0) ? bs::error_code(-r, osd_category()) :
                              bs::error_code());
  delete payload;
}

librados::AioCompletionImpl* create_aio_completion(
    std::unique_ptr<Op::Completion>&& c) {
  auto payload = new CompletionPayload{std::move(c)};

  auto impl = new librados::AioCompletionImpl();
  impl->set_complete_callback(payload, completion_callback_adapter);

  return impl;
}

int save_operation_size(int result, size_t* pval) {
  if (pval != NULL) {
    *pval = result;
  }
  return result;
}

int save_operation_ec(int result, boost::system::error_code* ec) {
  if (ec != NULL) {
    *ec = {std::abs(result), bs::system_category()};
  }
  return result;
}

} // anonymous namespace

Object::Object() {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t();
}

Object::Object(std::string&& s) {
  static_assert(impl_size >= sizeof(object_t));
  new (&impl) object_t(std::move(s));
}

Object::~Object() {
  reinterpret_cast<object_t*>(&impl)->~object_t();
}

Object::operator std::string_view() const {
  return std::string_view(reinterpret_cast<const object_t*>(&impl)->name);
}

struct IOContextImpl {
  object_locator_t oloc;
  snapid_t snap_seq = CEPH_NOSNAP;
  SnapContext snapc;
};

IOContext::IOContext() {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl();
}

IOContext::IOContext(const IOContext& rhs) {
  static_assert(impl_size >= sizeof(IOContextImpl));
  new (&impl) IOContextImpl(*reinterpret_cast<const IOContextImpl*>(&rhs.impl));
}

IOContext::IOContext(int64_t _pool, std::string&& _ns)
  : IOContext() {
  pool(_pool);
  ns(std::move(_ns));
}

IOContext::~IOContext() {
  reinterpret_cast<IOContextImpl*>(&impl)->~IOContextImpl();
}

std::int64_t IOContext::pool() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.pool;
}

void IOContext::pool(std::int64_t _pool) {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.pool = _pool;
}

std::string_view IOContext::ns() const {
  return reinterpret_cast<const IOContextImpl*>(&impl)->oloc.nspace;
}

void IOContext::ns(std::string&& _ns) {
  reinterpret_cast<IOContextImpl*>(&impl)->oloc.nspace = std::move(_ns);
}

std::optional<std::uint64_t> IOContext::read_snap() const {
  auto& snap_seq = reinterpret_cast<const IOContextImpl*>(&impl)->snap_seq;
  if (snap_seq == CEPH_NOSNAP)
    return std::nullopt;
  else
    return snap_seq;
}
void IOContext::read_snap(std::optional<std::uint64_t> _snapid) {
  auto& snap_seq = reinterpret_cast<IOContextImpl*>(&impl)->snap_seq;
  snap_seq = _snapid.value_or(CEPH_NOSNAP);
}

std::optional<
  std::pair<std::uint64_t,
            std::vector<std::uint64_t>>> IOContext::write_snap_context() const {
  auto& snapc = reinterpret_cast<const IOContextImpl*>(&impl)->snapc;
  if (snapc.empty()) {
    return std::nullopt;
  } else {
    std::vector<uint64_t> v(snapc.snaps.begin(), snapc.snaps.end());
    return std::make_optional(std::make_pair(uint64_t(snapc.seq), v));
  }
}

void IOContext::write_snap_context(
  std::optional<std::pair<std::uint64_t, std::vector<std::uint64_t>>> _snapc) {
  auto& snapc = reinterpret_cast<IOContextImpl*>(&impl)->snapc;
  if (!_snapc) {
    snapc.clear();
  } else {
    SnapContext n(_snapc->first, { _snapc->second.begin(), _snapc->second.end()});
    if (!n.is_valid()) {
      throw bs::system_error(EINVAL,
                             bs::system_category(),
                             "Invalid snap context.");
    }

    snapc = n;
  }
}

bool operator ==(const IOContext& lhs, const IOContext& rhs) {
  auto l = reinterpret_cast<const IOContextImpl*>(&lhs.impl);
  auto r = reinterpret_cast<const IOContextImpl*>(&rhs.impl);
  return (l->oloc == r->oloc &&
          l->snap_seq == r->snap_seq &&
          l->snapc.seq == r->snapc.seq &&
          l->snapc.snaps == r->snapc.snaps);
}

bool operator !=(const IOContext& lhs, const IOContext& rhs) {
  return !(lhs == rhs);
}

Op::Op() {
  static_assert(Op::impl_size >= sizeof(librados::LRemObjectOperationImpl*));
  auto& o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o = new librados::LRemObjectOperationImpl();
  o->get();
}

Op::~Op() {
  auto& o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  if (o != nullptr) {
    o->put();
    o = nullptr;
  }
}

void Op::assert_exists() {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::LRemIoCtxImpl::assert_exists, _1, _7, _4));
}

void Op::assert_version(uint64_t ver) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
          &librados::LRemIoCtxImpl::assert_version, _1, _7, ver));
}

void Op::cmpext(uint64_t off, ceph::buffer::list&& cmp_bl, std::size_t* s) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  librados::ObjectOperationLRemImpl op = std::bind(
    &librados::LRemIoCtxImpl::cmpext, _1, _7, off, cmp_bl, _4);
  if (s != nullptr) {
    op = std::bind(
      save_operation_size, std::bind(op, _1, _2, _3, _4, _5, _6, _7), s);
  }
  o->ops.push_back(op);
}

std::size_t Op::size() const {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl* const *>(&impl);
  return o->ops.size();
}

void Op::set_fadvise_random() {
  // no-op
}

void Op::set_fadvise_sequential() {
  // no-op
}

void Op::set_fadvise_willneed() {
  // no-op
}

void Op::set_fadvise_dontneed() {
  // no-op
}

void Op::set_fadvise_nocache() {
  // no-op
}

void Op::balance_reads() {
  // no-op
}

void Op::localize_reads() {
  // no-op
}

void Op::exec(std::string_view cls, std::string_view method,
              const ceph::buffer::list& inbl,
              ceph::buffer::list* out,
              boost::system::error_code* ec) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);

  auto cls_handler = librados_stub::get_class_handler();
  librados::ObjectOperationLRemImpl op =
    [cls_handler, cls, method, inbl = const_cast<bufferlist&>(inbl), out]
    (librados::LRemIoCtxImpl* io_ctx, const std::string& oid, bufferlist* outbl,
     uint64_t snap_id, const SnapContext& snapc, uint64_t*, librados::LRemTransactionStateRef& trans) mutable -> int {
      return io_ctx->exec(
        trans, cls_handler, std::string(cls).c_str(),
        std::string(method).c_str(), inbl,
        (out != nullptr ? out : outbl), snap_id, snapc);
    };
  if (ec != nullptr) {
    op = std::bind(
      save_operation_ec, std::bind(op, _1, _2, _3, _4, _5, _6, _7), ec);
  }
  o->ops.push_back(op);
}

void Op::exec(std::string_view cls, std::string_view method,
              const ceph::buffer::list& inbl,
              boost::system::error_code* ec) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);

  auto cls_handler = librados_stub::get_class_handler();
  librados::ObjectOperationLRemImpl op =
    [cls_handler, cls, method, inbl = const_cast<bufferlist&>(inbl)]
    (librados::LRemIoCtxImpl* io_ctx, const std::string& oid, bufferlist* outbl,
     uint64_t snap_id, const SnapContext& snapc, uint64_t*, librados::LRemTransactionStateRef& trans) mutable -> int {
      return io_ctx->exec(
        trans, cls_handler, std::string(cls).c_str(),
        std::string(method).c_str(), inbl, outbl, snap_id, snapc);
    };
  if (ec != NULL) {
    op = std::bind(
      save_operation_ec, std::bind(op, _1, _2, _3, _4, _5, _6, _7), ec);
  }
  o->ops.push_back(op);
}

void ReadOp::read(size_t off, uint64_t len, ceph::buffer::list* out,
	          boost::system::error_code* ec) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  librados::ObjectOperationLRemImpl op;
  if (out != nullptr) {
    op = std::bind(
            &librados::LRemIoCtxImpl::read, _1, _7, len, off, out, _4, _6);
  } else {
    op = std::bind(
            &librados::LRemIoCtxImpl::read, _1, _7, len, off, _3, _4, _6);
  }

  if (ec != NULL) {
    op = std::bind(
      save_operation_ec, std::bind(op, _1, _2, _3, _4, _5, _6, _7), ec);
  }
  o->ops.push_back(op);
}

void ReadOp::sparse_read(uint64_t off, uint64_t len,
		         ceph::buffer::list* out,
		         std::vector<std::pair<std::uint64_t,
                                               std::uint64_t>>* extents,
		         boost::system::error_code* ec) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  librados::ObjectOperationLRemImpl op =
    [off, len, out, extents]
    (librados::LRemIoCtxImpl* io_ctx, const std::string& oid, bufferlist* outbl,
     uint64_t snap_id, const SnapContext& snapc, uint64_t*, librados::LRemTransactionStateRef& trans) mutable -> int {
      std::map<uint64_t,uint64_t> m;
      int r = io_ctx->sparse_read(
        trans, off, len, &m, (out != nullptr ? out : outbl), snap_id);
      if (r >= 0 && extents != nullptr) {
        extents->clear();
        extents->insert(extents->end(), m.begin(), m.end());
      }
      return r;
    };
  if (ec != NULL) {
    op = std::bind(save_operation_ec,
                     std::bind(op, _1, _2, _3, _4, _5, _6, _7), ec);
  }
  o->ops.push_back(op);
}

void ReadOp::list_snaps(SnapSet* snaps, bs::error_code* ec) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  librados::ObjectOperationLRemImpl op =
    [snaps]
    (librados::LRemIoCtxImpl* io_ctx, const std::string& oid, bufferlist*,
     uint64_t, const SnapContext&, uint64_t*, librados::LRemTransactionStateRef& trans) mutable -> int {
      librados::snap_set_t snap_set;
      int r = io_ctx->list_snaps(trans, &snap_set);
      if (r >= 0 && snaps != nullptr) {
        *snaps = {};
        snaps->seq = snap_set.seq;
        snaps->clones.reserve(snap_set.clones.size());
        for (auto& clone : snap_set.clones) {
          neorados::CloneInfo clone_info;
          clone_info.cloneid = clone.cloneid;
          clone_info.snaps = clone.snaps;
          clone_info.overlap = clone.overlap;
          clone_info.size = clone.size;
          snaps->clones.push_back(clone_info);
        }
      }
      return r;
    };
  if (ec != NULL) {
    op = std::bind(save_operation_ec,
                   std::bind(op, _1, _2, _3, _4, _5, _6, _7), ec);
  }
  o->ops.push_back(op);
}

void WriteOp::create(bool exclusive) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::LRemIoCtxImpl::create, _1, _7, exclusive, _5));
}

void WriteOp::write(uint64_t off, ceph::buffer::list&& bl) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::LRemIoCtxImpl::write, _1, _7, bl, bl.length(), off, _5));
}

void WriteOp::write_full(ceph::buffer::list&& bl) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::LRemIoCtxImpl::write_full, _1, _7, bl, _5));
}

void WriteOp::remove() {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::LRemIoCtxImpl::remove, _1, _7, _5));
}

void WriteOp::truncate(uint64_t off) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::LRemIoCtxImpl::truncate, _1, _7, off, _5));
}

void WriteOp::zero(uint64_t off, uint64_t len) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::LRemIoCtxImpl::zero, _1, _7, off, len, _5));
}

void WriteOp::writesame(std::uint64_t off, std::uint64_t write_len,
                        ceph::buffer::list&& bl) {
  auto o = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&impl);
  o->ops.push_back(std::bind(
    &librados::LRemIoCtxImpl::writesame, _1, _7, bl, write_len, off, _5));
}

void WriteOp::set_alloc_hint(uint64_t expected_object_size,
		             uint64_t expected_write_size,
		             alloc_hint::alloc_hint_t flags) {
  // no-op
}

RADOS::RADOS() = default;

RADOS::RADOS(RADOS&&) = default;

RADOS::RADOS(std::unique_ptr<detail::Client> impl)
  : impl(std::move(impl)) {
}

RADOS::~RADOS() = default;

RADOS RADOS::make_with_librados(librados::Rados& rados) {
  auto lrem_rados_client = reinterpret_cast<librados::LRemRadosClient*>(
    rados.client);
  return RADOS{std::make_unique<detail::Client>(lrem_rados_client)};
}

CephContext* neorados::RADOS::cct() {
  return impl->lrem_rados_client->cct();
}

boost::asio::io_context& neorados::RADOS::get_io_context() {
  return impl->io_context;
}

boost::asio::io_context::executor_type neorados::RADOS::get_executor() const {
  return impl->io_context.get_executor();
}

void RADOS::execute(const Object& o, const IOContext& ioc, ReadOp&& op,
                    ceph::buffer::list* bl, std::unique_ptr<Op::Completion> c,
                    uint64_t* objver, const blkin_trace_info* trace_info) {
  auto io_ctx = impl->get_io_ctx(ioc);
  if (io_ctx == nullptr) {
    c->dispatch(std::move(c), osdc_errc::pool_dne);
    return;
  }

  auto ops = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&op.impl);

  auto snap_id = CEPH_NOSNAP;
  auto opt_snap_id = ioc.read_snap();
  if (opt_snap_id) {
    snap_id = *opt_snap_id;
  }

  auto completion = create_aio_completion(std::move(c));
  auto r = io_ctx->aio_operate_read(std::string{o}, *ops, completion, 0U, bl,
                                    snap_id, objver);
  ceph_assert(r == 0);
}

void RADOS::execute(const Object& o, const IOContext& ioc, WriteOp&& op,
                    std::unique_ptr<Op::Completion> c, uint64_t* objver,
                    const blkin_trace_info* trace_info) {
  auto io_ctx = impl->get_io_ctx(ioc);
  if (io_ctx == nullptr) {
    c->dispatch(std::move(c), osdc_errc::pool_dne);
    return;
  }

  auto ops = *reinterpret_cast<librados::LRemObjectOperationImpl**>(&op.impl);

  SnapContext snapc;
  auto opt_snapc = ioc.write_snap_context();
  if (opt_snapc) {
    snapc.seq = opt_snapc->first;
    snapc.snaps.assign(opt_snapc->second.begin(), opt_snapc->second.end());
  }

  auto completion = create_aio_completion(std::move(c));
  auto r = io_ctx->aio_operate(std::string{o}, *ops, completion, &snapc, 0U);
  ceph_assert(r == 0);
}

void RADOS::mon_command(std::vector<std::string> command,
                        const bufferlist& bl,
                        std::string* outs, bufferlist* outbl,
                        std::unique_ptr<Op::Completion> c) {
  auto r = impl->lrem_rados_client->mon_command(command, bl, outbl, outs);
  c->post(std::move(c),
          (r < 0 ? bs::error_code(-r, osd_category()) : bs::error_code()));
}

void RADOS::blocklist_add(std::string_view client_address,
                          std::optional<std::chrono::seconds> expire,
                          std::unique_ptr<SimpleOpComp> c) {
  auto r = impl->lrem_rados_client->blocklist_add(
    std::string(client_address), expire.value_or(0s).count());
  c->post(std::move(c),
          (r < 0 ? bs::error_code(-r, mon_category()) : bs::error_code()));
}

void RADOS::wait_for_latest_osd_map(std::unique_ptr<Op::Completion> c) {
  auto r = impl->lrem_rados_client->wait_for_latest_osd_map();
  c->dispatch(std::move(c),
              (r < 0 ? bs::error_code(-r, osd_category()) :
                       bs::error_code()));
}

} // namespace neorados

