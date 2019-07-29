#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include "rados.h"

namespace py = pybind11;

using StringVector = std::vector<std::string>;
using Int2StringPairVector = std::vector<std::pair<int64_t, std::string>>;

PYBIND11_MAKE_OPAQUE(StringVector);
PYBIND11_MAKE_OPAQUE(Int2StringPairVector);

namespace radosx {

PYBIND11_MODULE(radosx, m) {
  py::bind_vector<StringVector>(m, "StringVector");
  py::bind_vector<Int2StringPairVector>(m, "Int2StringPairVector");

  //
  // IoCtx
  //
  py::class_<librados::IoCtx>(m, "IoCtx")
      .def(py::init<>());

  py::class_<xIoCtx> ioctx(m, "xIoCtx");

  ioctx.def_readonly("ioctx", &xIoCtx::ioctx);
  ioctx.def(py::init<>());
  ioctx.def("get_id", &xIoCtx::get_id);

  //
  // Rados
  //
  py::class_<librados::Rados>(m, "Rados")
      .def(py::init<>());

  py::class_<xRados> rados(m, "xRados");

  rados.def_readonly("rados", &xRados::rados);
  rados.def(py::init<>());
  rados.def("init", &xRados::init);
  rados.def("init2", &xRados::init2);
  rados.def("conf_read_file", &xRados::conf_read_file);
  rados.def("connect", &xRados::connect);
  rados.def("shutdown", &xRados::shutdown);
  rados.def("pool_list", &xRados::pool_list);
  rados.def("pool_list2", &xRados::pool_list2);
  rados.def("ioctx_create", &xRados::ioctx_create);
}

}
