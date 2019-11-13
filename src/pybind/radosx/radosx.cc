#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include "rados/librados.hpp"

#include <list>
#include <string>
#include <vector>

namespace py = pybind11;

using Vector_string = std::vector<std::string>;
using Vector_pair_int64_t_string = std::vector<std::pair<int64_t, std::string>>;

PYBIND11_MAKE_OPAQUE(Vector_string);
PYBIND11_MAKE_OPAQUE(Vector_pair_int64_t_string);

namespace radosx {

using namespace librados;

constexpr uint64_t CEPH_NOSNAP = ((uint64_t)(-2));

PYBIND11_MODULE(radosx, m) {
  py::bind_vector<Vector_string>(m, "Vector_string");
  py::bind_vector<Vector_pair_int64_t_string>(m, "Vector_pair_int64_t_string");

  m.attr("CEPH_NOSNAP") = py::int_(CEPH_NOSNAP);

  //
  // IoCtx
  //
  {
    py::class_<IoCtx> cls(m, "IoCtx");
    cls.def(py::init<>());
    cls.def("get_id", &IoCtx::get_id);
  }

  //
  // Rados
  //
  {
    py::class_<Rados> cls(m, "Rados");
    cls.def(py::init<>());
    cls.def("init", &Rados::init);
    cls.def("init2", &Rados::init2);
    // Transfer a c++ object between cython and pybind11 using python capsules
    // https://stackoverflow.com/questions/57115655/transfer-a-c-object-between-cython-and-pybind11-using-python-capsules
    // cct is a global static pointer, reference counting is not needed,
    // so using py::handle instead of py::object or py::capsule
    cls.def("init_with_context", [](Rados& self, py::handle* capsule) {
      auto* ptr = capsule->ptr();
      auto* cct = reinterpret_cast<rados_config_t*>(PyCapsule_GetPointer(ptr, nullptr));
      self.init_with_context(reinterpret_cast<config_t*>(cct));
    });
    cls.def("conf_read_file", &Rados::conf_read_file);
    cls.def("conf_set", &Rados::conf_set);
    cls.def("connect", &Rados::connect);
    cls.def("shutdown", &Rados::shutdown);
    cls.def("pool_lookup", &Rados::pool_lookup);
    cls.def("pool_list", [](Rados& self, std::vector<std::string>& v) {
      std::list<std::string> l;
      int r = self.pool_list(l);
      if (r < 0) {
        return r;
      }

      std::vector<std::string> tv{
        std::make_move_iterator(std::begin(l)),
        std::make_move_iterator(std::end(l))
      };
      v.swap(tv);
      return 0;
    });
    cls.def("pool_list2", [](Rados& self, std::vector<std::pair<int64_t, std::string>>& v) {
      std::list<std::pair<int64_t, std::string>> l;
      int r = self.pool_list2(l);
      if (r < 0) {
        return r;
      }

      std::vector<std::pair<int64_t, std::string>> tv{
        std::make_move_iterator(std::begin(l)),
        std::make_move_iterator(std::end(l))
      };
      v.swap(tv);
      return 0;
    });
    cls.def("ioctx_create", &Rados::ioctx_create);
    cls.def("ioctx_create2", &Rados::ioctx_create2);
  }

} // PYBIND11_MODULE(radosx, m)

} // namespace radosx
