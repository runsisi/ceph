#include <pybind11/pybind11.h>

#include "rados/librados.hpp"

#include <algorithm>
#include <list>
#include <string>
#include <vector>

namespace py = pybind11;

namespace radosx {

using namespace librados;

// it is defined as uint64_t in ceph C++ code
constexpr int64_t CEPH_NOSNAP = ((int64_t)(-2));

class xRados : public Rados {
public:
  ~xRados() {
    shutdown();
  }

  int from_rados(py::handle h_rados) {
    int r = require_state({"shutdown"});
    if (r < 0) {
      return -EEXIST;
    }
    // if the rados.Rados instance was shutdown by Cython code, then the
    // underlying RadosClient instance was deleted when we are still in
    // "connected" state, so always remember to shutdown xRados instance
    // before rados.Rados instance
    auto* ptr = h_rados.ptr();
    auto* c_rados = reinterpret_cast<rados_t*>(PyCapsule_GetPointer(ptr, nullptr));
    Rados::from_rados_t(c_rados, *this);
    state = "connected";
    return 0;
  }

  // assume client.id
  int init(const char * const id) {
    int r = Rados::init(id);
    if (r < 0) {
      return r;
    }
    state = "configuring";
    return 0;
  }
  // parse type.id from name
  int init2(const char * const name) {
    int r = Rados::init2(name, "", 0);
    if (r < 0) {
      return r;
    }
    state = "configuring";
    return 0;
  }
  int init_with_context(config_t cct) {
    int r = Rados::init_with_context(cct);
    if (r < 0) {
      return r;
    }
    state = "configuring";
    return 0;
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

  std::string get_state() const {
    return state;
  }

private:
  std::string state = "shutdown";

  int require_state(std::list<std::string>&& states) const {
    if (std::find_if(states.begin(), states.end(), [this](const std::string& s) {
      return s == state;
    }) == states.end()) {
      return -EBADF;
    }
    return 0;
  }
};

PYBIND11_MODULE(radosx, m) {

  m.attr("CEPH_NOSNAP") = py::int_(CEPH_NOSNAP);

  //
  // Rados
  //
  {
    py::class_<xRados> cls(m, "xRados");
    cls.def(py::init<>());
    cls.def_property_readonly("state", &xRados::get_state);
    cls.def("from_rados", &xRados::from_rados);
    cls.def("init", &xRados::init);
    cls.def("init2", &xRados::init2);
    cls.def("init_with_context", [](xRados& self, py::handle h_rados_cct) {
      auto* ptr = h_rados_cct.ptr();
      auto* c_cct = reinterpret_cast<config_t*>(PyCapsule_GetPointer(ptr, nullptr));
      return self.init_with_context(c_cct);
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
    cls.def(py::init([](py::handle h_rados_ioctx) {
      auto* ptr = h_rados_ioctx.ptr();
      auto* c_ioctx = reinterpret_cast<rados_ioctx_t*>(PyCapsule_GetPointer(ptr, nullptr));
      auto ioctx = new IoCtx{};
      IoCtx::from_rados_ioctx_t(c_ioctx, *ioctx);
      return std::unique_ptr<IoCtx>{ioctx};
    }));
    cls.def("__enter__", [](IoCtx& self) {
      return self;
    });
    cls.def("__exit__", [](IoCtx& self, py::args) {
      self.close();
    });
    cls.def("from_rados_ioctx", [](IoCtx& self, py::handle h_rados_ioctx) {
      if (self.is_valid()) {
        return -EEXIST;
      }
      auto* ptr = h_rados_ioctx.ptr();
      auto* c_ioctx = reinterpret_cast<rados_ioctx_t*>(PyCapsule_GetPointer(ptr, nullptr));
      IoCtx::from_rados_ioctx_t(c_ioctx, self);
      return 0;
    });
    cls.def("cct", [](IoCtx& self) {
      if (!self.is_valid()) {
        return py::cast(nullptr);
      }
      auto cct = self.cct();
      return py::cast(cct);
    }, py::return_value_policy::reference);
    cls.def("set_namespace", [](IoCtx& self, const std::string nspace) {
      if (!self.is_valid()) {
        return -EBADF;
      }
      self.set_namespace(nspace);
      return 0;
    });
    cls.def("get_namespace", [](const IoCtx& self) {
      if (!self.is_valid()) {
        return py::cast(nullptr);
      }
      auto nspace = self.get_namespace();
      return py::cast(nspace);
    });
    cls.def("get_id", [](IoCtx& self) {
      if (!self.is_valid()) {
        return int64_t(-1);
      }
      return self.get_id();
    });
  }

} // PYBIND11_MODULE(radosx, m)

} // namespace radosx
