#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include "rbd.h"

namespace py = pybind11;

using String2StringMap = std::map<std::string, std::string>;

PYBIND11_MAKE_OPAQUE(String2StringMap);

namespace rbdx {

PYBIND11_MODULE(rbdx, m) {
  py::bind_map<String2StringMap>(m, "String2StringMap");

  //
  // RBD
  //
  py::class_<librbd::RBD>(m, "RBD")
      .def(py::init<>());

  py::class_<xRBD> rbd(m, "xRBD");

  rbd.def_readonly("rbd", &xRBD::rbd);
  rbd.def(py::init<>());
  rbd.def("x_list", &xRBD::x_list);
}

}
