// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LREM_CLASS_HANDLER_H
#define CEPH_LREM_CLASS_HANDLER_H

#include "objclass/objclass.h"
#include "osd/osd_types.h"
#include "common/snap_types.h"
#include <boost/shared_ptr.hpp>
#include <list>
#include <map>
#include <string>

#include "LRemTransaction.h"

namespace librados
{

class LRemIoCtxImpl;

class LRemClassHandler {
public:

  LRemClassHandler();
  ~LRemClassHandler();

  struct MethodContext {
    ~MethodContext();

    LRemIoCtxImpl *io_ctx_impl;
    std::string oid;
    uint64_t snap_id;
    SnapContext snapc;
    LRemTransactionStateRef trans;
    object_info_t oi;
  };
  typedef boost::shared_ptr<MethodContext> SharedMethodContext;

  struct Method {
    int flags;
    cls_method_cxx_call_t class_call;
  };
  typedef boost::shared_ptr<Method> SharedMethod;
  typedef std::map<std::string, SharedMethod> Methods;
  typedef std::map<std::string, cls_cxx_filter_factory_t> Filters;

  struct Class {
    Methods methods;
    Filters filters;
  };
  typedef boost::shared_ptr<Class> SharedClass;

  void open_all_classes();

  int create(const std::string &name, cls_handle_t *handle);
  int create_method(cls_handle_t hclass, const char *method,
                    int flags,
                    cls_method_cxx_call_t class_call,
                    cls_method_handle_t *handle);
  cls_method_cxx_call_t get_method(const std::string &cls,
                                   const std::string &method,
                                   bool *write);
  SharedMethodContext get_method_context(LRemIoCtxImpl *io_ctx_impl,
                                         const std::string &oid,
                                         uint64_t snap_id,
                                         const SnapContext &snapc,
                                         LRemTransactionStateRef& trans);

  int create_filter(cls_handle_t hclass, const std::string& filter_name,
		    cls_cxx_filter_factory_t fn);

private:

  typedef std::map<std::string, SharedClass> Classes;
  typedef std::list<void*> ClassHandles;

  Classes m_classes;
  ClassHandles m_class_handles;
  Filters m_filters;

  void open_class(const std::string& name, const std::string& path);

};

} // namespace librados

#endif // CEPH_LREM_CLASS_HANDLER_H
