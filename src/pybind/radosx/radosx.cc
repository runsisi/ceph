#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include "rados/librados.hpp"

#include <algorithm>
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

class xRados : public Rados {
public:
  // assume client.id
  int init(const char * const id) {
    state = "configuring";
    return Rados::init(id);
  }
  // parse type.id from name
  int init2(const char * const name) {
    state = "configuring";
    return Rados::init2(name, "", 0);
  }
  int init_with_context(config_t cct) {
    state = "configuring";
    return Rados::init_with_context(cct);
  }

  config_t cct() {
    int r = require_state({"configuring", "connected"});
    if (r < 0) {
      return nullptr;
    }
    return Rados::cct();
  }

  int conf_read_file(const char * const path) const {
    int r = require_state({"configuring", "connected"});
    if (r < 0) {
      return r;
    }
    return Rados::conf_read_file(path);
  }
  int conf_set(const char *option, const char *value) {
    int r = require_state({"configuring", "connected"});
    if (r < 0) {
      return r;
    }
    return Rados::conf_set(option, value);
  }

  int connect() {
    int r = require_state({"configuring"});
    if (r < 0) {
      return r;
    }
    r = Rados::connect();
    if (r < 0) {
      return r;
    }
    state = "connected";
    return 0;
  }
  void shutdown() {
    if (state != "shutdown") {
      Rados::shutdown();
      state = "shutdown";
    }
  }

  int ioctx_create(const char *name, IoCtx &pioctx) {
    int r = require_state({"connected"});
    if (r < 0) {
      return r;
    }
    return Rados::ioctx_create(name, pioctx);
  }
  int ioctx_create2(int64_t pool_id, IoCtx &pioctx) {
    int r = require_state({"connected"});
    if (r < 0) {
      return r;
    }
    return Rados::ioctx_create2(pool_id, pioctx);
  }

private:
  std::string state;

  int require_state(std::list<std::string>&& states) const {
    if (std::find_if(states.begin(), states.end(), [this](const std::string& s) {
      return s == state;
    }) == states.end()) {
      return -EINVAL;
    }
    return 0;
  }
};

PYBIND11_MODULE(radosx, m) {
  py::bind_vector<Vector_string>(m, "Vector_string");
  py::bind_vector<Vector_pair_int64_t_string>(m, "Vector_pair_int64_t_string");

  m.attr("CEPH_NOSNAP") = py::int_(CEPH_NOSNAP);

  //
  // Rados
  //
  {
    py::class_<xRados> cls(m, "xRados");
    cls.def(py::init<>());
    cls.def("init", &xRados::init);
    cls.def("init2", &xRados::init2);
    cls.def("init_with_context", [](xRados& self, py::handle h) {
      auto* ptr = h.ptr();
      auto* cct = reinterpret_cast<rados_config_t*>(PyCapsule_GetPointer(ptr, nullptr));
      return self.init_with_context(reinterpret_cast<config_t*>(cct));
    });
    cls.def("cct", [](xRados& self) {
      auto cct = self.cct();
      // default policy is return_value_policy::automatic_reference
      return py::cast(cct);
    }, py::return_value_policy::reference);
    cls.def("conf_read_file", &xRados::conf_read_file);
    cls.def("conf_set", &xRados::conf_set);
    cls.def("connect", &xRados::connect);
    cls.def("shutdown", &xRados::shutdown);
    cls.def("ioctx_create", &xRados::ioctx_create);
    cls.def("ioctx_create2", &xRados::ioctx_create2);
  }

  //
  // IoCtx
  //
  {
    py::class_<IoCtx> cls(m, "xIoCtx");
    cls.def(py::init<>());
    cls.def("get_id", [](IoCtx& self) {
      if (!self.is_valid()) {
        return int64_t(-1);
      }
      return self.get_id();
    });
    cls.def("cct", [](IoCtx& self) {
      if (!self.is_valid()) {
        return py::cast(nullptr);
      }
      return py::cast(self.cct());
    });
  }
} // PYBIND11_MODULE(radosx, m)

} // namespace radosx
