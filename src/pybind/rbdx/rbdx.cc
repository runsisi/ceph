#include <pybind11/pybind11.h>
#include <pybind11/stl_bind.h>

#include "rados/librados.hpp"
#include "rbd/librbdx.hpp"

#include <list>
#include <map>
#include <set>
#include <string>
#include <type_traits>

#include "nlohmann/json.hpp"

namespace py = pybind11;

using Vector_string = std::vector<std::string>;

// list_du, reuse of list_info
using Map_string_2_pair_du_info_t_int = std::map<std::string, std::pair<librbdx::image_info_t, int>>;
// list
using Map_string_2_string = std::map<std::string, std::string>;
// list_info
using Map_string_2_pair_image_info_t_int = std::map<std::string, std::pair<librbdx::image_info_t, int>>;
// list_info_v2
using Map_string_2_pair_image_info_v3_t_int = std::map<std::string, std::pair<librbdx::image_info_v2_t, int>>;
// trash_list
using Map_string_2_trash_info_t = std::map<std::string, librbdx::trash_info_t>;
// clove v1 child_list
using Map_parent_spec_t_2_vector_string = std::map<librbdx::parent_spec_t, std::vector<std::string>>;


PYBIND11_MAKE_OPAQUE(Vector_string);

// list_du
PYBIND11_MAKE_OPAQUE(Map_string_2_pair_du_info_t_int);
// list
PYBIND11_MAKE_OPAQUE(Map_string_2_string);
// list_info
PYBIND11_MAKE_OPAQUE(Map_string_2_pair_image_info_t_int);
// list_info_v2
PYBIND11_MAKE_OPAQUE(Map_string_2_pair_image_info_v3_t_int);
// trash_list
PYBIND11_MAKE_OPAQUE(Map_string_2_trash_info_t);
// clone v1 child_list
PYBIND11_MAKE_OPAQUE(Map_parent_spec_t_2_vector_string);

namespace {

// is_string
template<typename T>
struct is_string :
    std::integral_constant<bool,
      std::is_same<char*, typename std::decay<T>::type>::value ||
      std::is_same<const char*, typename std::decay<T>::type>::value
    > {};

template<>
struct is_string<std::string> : std::true_type {};

// is_pair
template <typename T>
struct is_pair : std::false_type {};

template <typename T1, typename T2>
struct is_pair<std::pair<T1, T2>> : std::true_type {};

// is_sequence
template <typename T>
struct is_sequence : std::false_type {};

template <typename... Ts> struct is_sequence<std::list<Ts...>> : std::true_type {};
template <typename... Ts> struct is_sequence<std::set<Ts...>> : std::true_type {};
template <typename... Ts> struct is_sequence<std::vector<Ts...>> : std::true_type {};

}

namespace {

using namespace librbdx;
using json = nlohmann::json;

// forward declaration, otherwise will have errors like the following
// error: no matching function for call to ‘json_fmt(const librbdx::snap_info_t&)’
template <typename T,
  typename std::enable_if<std::is_arithmetic<T>::value ||
      is_string<T>::value, std::nullptr_t>::type=nullptr
>
auto json_fmt(const T& o) -> decltype(o);

template <typename T,
  typename std::enable_if<std::is_enum<T>::value, std::nullptr_t>::type=nullptr
>
std::string json_fmt(const T& o);

template <typename T,
  typename std::enable_if<is_pair<T>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const T& o);

template <typename T,
  typename std::enable_if<is_sequence<T>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const T& o);

template <typename K, typename V, typename... Ts,
  typename std::enable_if<std::is_arithmetic<K>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o);

template <typename K, typename V, typename... Ts,
  typename std::enable_if<is_string<K>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o);

template <typename K, typename V, typename... Ts,
  typename std::enable_if<
    std::is_same<K, parent_spec_t>::value,
    std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o);

json json_fmt(const timespec& o);
json json_fmt(const snapc_t& o);
json json_fmt(const parent_spec_t& o);
json json_fmt(const parent_info_t& o);
json json_fmt(const child_t& o);
json json_fmt(const qos_t& o);
json json_fmt(const snap_info_t& o);
json json_fmt(const snap_info_v2_t& o);
json json_fmt(const image_info_t& o);
json json_fmt(const trash_info_t& o);

template <typename T,
  typename std::enable_if<std::is_arithmetic<T>::value ||
      is_string<T>::value, std::nullptr_t>::type=nullptr
>
auto json_fmt(const T& o) -> decltype(o) {
  return o;
}

template <typename T,
  typename std::enable_if<std::is_enum<T>::value, std::nullptr_t>::type=nullptr
>
std::string json_fmt(const T& o) {
  return to_str(o);
}

template <typename T,
  typename std::enable_if<is_pair<T>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const T& o) {
  json j = json::array({});
  j.push_back(json_fmt(o.first));
  j.push_back(json_fmt(o.second));
  return std::move(j);
}

template <typename T,
  typename std::enable_if<is_sequence<T>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const T& o) {
  json j = json::array({});
  for (auto& i : o) {
    j.push_back(json_fmt(i));
  }
  return std::move(j);
}

template <typename K, typename V, typename... Ts,
  typename std::enable_if<std::is_arithmetic<K>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o) {
  json j = json::object({});
  for (auto& it : o) {
    auto k = std::to_string(it.first);
    j[k] = json_fmt(it.second);
  }
  return std::move(j);
}

template <typename K, typename V, typename... Ts,
  typename std::enable_if<is_string<K>::value, std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o) {
  json j = json::object({});
  for (auto& it : o) {
    auto k = it.first;
    j[k] = json_fmt(it.second);
  }
  return std::move(j);
}

template <typename K, typename V, typename... Ts,
  typename std::enable_if<
    std::is_same<K, parent_spec_t>::value,
    std::nullptr_t>::type=nullptr
>
json json_fmt(const std::map<K, V, Ts...>& o) {
  json j = json::object({});
  for (auto& it : o) {
    auto k = to_str(it.first);
    j[k] = json_fmt(it.second);
  }
  return std::move(j);
}

json json_fmt(const timespec& o) {
  json j = json::object({});
  j["tv_sec"] = json_fmt(o.tv_sec);
  j["tv_nsec"] = json_fmt(o.tv_nsec);
  return std::move(j);
}

json json_fmt(const snapc_t& o) {
  json j = json::object({});
  j["seq"] = json_fmt(o.seq);
  j["snaps"] = json_fmt(o.snaps);
  return std::move(j);
}

json json_fmt(const parent_spec_t& o) {
  json j = json::object({});
  j["pool_id"] = json_fmt(o.pool_id);
  j["image_id"] = json_fmt(o.image_id);
  j["snap_id"] = json_fmt(o.snap_id);
  return std::move(j);
}

json json_fmt(const parent_info_t& o) {
  json j = json::object({});
  j["spec"] = json_fmt(o.spec);
  j["overlap"] = json_fmt(o.overlap);
  return std::move(j);
}

json json_fmt(const child_t& o) {
  json j = json::object({});
  j["pool_id"] = json_fmt(o.pool_id);
  j["image_id"] = json_fmt(o.image_id);
  return std::move(j);
}

json json_fmt(const qos_t& o) {
  json j = json::object({});
  j["iops"] = json_fmt(o.iops);
  j["bps"] = json_fmt(o.bps);
  return std::move(j);
}

json json_fmt(const snap_info_t& o) {
  json j = json::object({});
  j["id"] = json_fmt(o.id);
  j["name"] = json_fmt(o.name);
  j["snap_ns_type"] = json_fmt(o.snap_ns_type);
  j["size"] = json_fmt(o.size);
  j["flags"] = json_fmt(o.flags);
  j["protection_status"] = json_fmt(o.protection_status);
  j["timestamp"] = json_fmt(o.timestamp);
  return std::move(j);
}

json json_fmt(const snap_info_v2_t& o) {
  json j = json::object({});
  j["id"] = json_fmt(o.id);
  j["name"] = json_fmt(o.name);
  j["snap_ns_type"] = json_fmt(o.snap_ns_type);
  j["size"] = json_fmt(o.size);
  j["flags"] = json_fmt(o.flags);
  j["protection_status"] = json_fmt(o.protection_status);
  j["timestamp"] = json_fmt(o.timestamp);
  j["du"] = json_fmt(o.du);
  j["dirty"] = json_fmt(o.dirty);
  return std::move(j);
}

json json_fmt(const image_info_t& o) {
  json j = json::object({});
  j["id"] = json_fmt(o.id);
  j["name"] = json_fmt(o.name);
  j["order"] = json_fmt(o.order);
  j["size"] = json_fmt(o.size);
  j["stripe_unit"] = json_fmt(o.stripe_unit);
  j["stripe_count"] = json_fmt(o.stripe_count);
  j["features"] = json_fmt(o.features);
  j["flags"] = json_fmt(o.flags);
  j["snapc"] = json_fmt(o.snapc);
  j["snaps"] = json_fmt(o.snaps);
  j["parent"] = json_fmt(o.parent);
  j["timestamp"] = json_fmt(o.timestamp);
  j["data_pool_id"] = json_fmt(o.data_pool_id);
  j["watchers"] = json_fmt(o.watchers);
  j["qos"] = json_fmt(o.qos);
  j["du"] = json_fmt(o.du);
  return std::move(j);
}

json json_fmt(const image_info_v2_t& o) {
  json j = json::object({});
  j["id"] = json_fmt(o.id);
  j["name"] = json_fmt(o.name);
  j["order"] = json_fmt(o.order);
  j["size"] = json_fmt(o.size);
  j["stripe_unit"] = json_fmt(o.stripe_unit);
  j["stripe_count"] = json_fmt(o.stripe_count);
  j["features"] = json_fmt(o.features);
  j["flags"] = json_fmt(o.flags);
  j["snapc"] = json_fmt(o.snapc);
  j["snaps"] = json_fmt(o.snaps);
  j["parent"] = json_fmt(o.parent);
  j["timestamp"] = json_fmt(o.timestamp);
  j["data_pool_id"] = json_fmt(o.data_pool_id);
  j["watchers"] = json_fmt(o.watchers);
  j["qos"] = json_fmt(o.qos);
  j["du"] = json_fmt(o.du);
  return std::move(j);
}

json json_fmt(const trash_info_t& o) {
  json j = json::object({});
  j["id"] = json_fmt(o.id);
  j["name"] = json_fmt(o.name);
  j["source"] = json_fmt(o.source);
  j["deletion_time"] = json_fmt(o.deletion_time);
  j["deferment_end_time"] = json_fmt(o.deferment_end_time);
  return std::move(j);
}

}

namespace rbdx {

using namespace librbdx;
using json = nlohmann::json;

constexpr int json_indent = 4;

PYBIND11_MODULE(rbdx, m) {

  {
    auto b = py::bind_vector<Vector_string>(m, "Vector_string");
    b.def("__repr__", [](const Vector_string& self) {
      return json_fmt(self).dump(json_indent);
    });
  }
  {
    auto b = py::bind_map<Map_string_2_string>(m, "Map_string_2_string");
    b.def("__repr__", [](const Map_string_2_string& self) {
      return json_fmt(self).dump(json_indent);
    });
  }
  {
    auto b = py::bind_map<Map_string_2_pair_du_info_t_int>(m, "Map_string_2_pair_du_info_t_int");
    b.def("__repr__", [](const Map_string_2_pair_du_info_t_int& self) {
      return json_fmt(self).dump(json_indent);
    });
  }
  {
    auto b = py::bind_map<Map_string_2_pair_image_info_t_int>(m, "Map_string_2_pair_image_info_t_int");
    b.def("__repr__", [](const Map_string_2_pair_image_info_t_int& self) {
      return json_fmt(self).dump(json_indent);
    });
  }
  {
    auto b = py::bind_map<Map_string_2_pair_image_info_v2_t_int>(m, "Map_string_2_pair_image_info_v2_t_int");
    b.def("__repr__", [](const Map_string_2_pair_image_info_v3_t_int& self) {
      return json_fmt(self).dump(json_indent);
    });
  }
  {
    auto b = py::bind_map<Map_string_2_trash_info_t>(m, "Map_string_2_trash_info_t");
    b.def("__repr__", [](const Map_string_2_trash_info_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }
  {
    auto b = py::bind_map<Map_parent_spec_t_2_vector_string>(m, "Map_parent_spec_t_2_vector_string");
    b.def("__repr__", [](const Map_parent_spec_t_2_vector_string& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::enum_<snap_ns_type_t> kind(m, "snap_ns_type_t", py::arithmetic());
    kind.value("SNAPSHOT_NAMESPACE_TYPE_USER", snap_ns_type_t::SNAPSHOT_NAMESPACE_TYPE_USER);
    kind.value("SNAPSHOT_NAMESPACE_TYPE_GROUP", snap_ns_type_t::SNAPSHOT_NAMESPACE_TYPE_GROUP);
    kind.value("SNAPSHOT_NAMESPACE_TYPE_TRASH", snap_ns_type_t::SNAPSHOT_NAMESPACE_TYPE_TRASH);
    kind.export_values();
  }

  {
    py::enum_<snap_protection_status_t> kind(m, "snap_protection_status_t", py::arithmetic());
    kind.value("PROTECTION_STATUS_UNPROTECTED", snap_protection_status_t::PROTECTION_STATUS_UNPROTECTED);
    kind.value("PROTECTION_STATUS_UNPROTECTING", snap_protection_status_t::PROTECTION_STATUS_UNPROTECTING);
    kind.value("PROTECTION_STATUS_PROTECTED", snap_protection_status_t::PROTECTION_STATUS_PROTECTED);
    kind.value("PROTECTION_STATUS_LAST", snap_protection_status_t::PROTECTION_STATUS_LAST);
    kind.export_values();
  }

  {
    py::enum_<trash_source_t> kind(m, "trash_source_t", py::arithmetic());
    kind.value("TRASH_IMAGE_SOURCE_USER", trash_source_t::TRASH_IMAGE_SOURCE_USER);
    kind.value("TRASH_IMAGE_SOURCE_MIRRORING", trash_source_t::TRASH_IMAGE_SOURCE_MIRRORING);
    kind.value("TRASH_IMAGE_SOURCE_MIGRATION", trash_source_t::TRASH_IMAGE_SOURCE_MIGRATION);
    kind.value("TRASH_IMAGE_SOURCE_REMOVING", trash_source_t::TRASH_IMAGE_SOURCE_REMOVING);
    kind.export_values();
  }

  {
    py::class_<timespec> cls(m, "timespec_t");
    cls.def(py::init<>());
    cls.def_readonly("tv_sec", &timespec::tv_sec);
    cls.def_readonly("tv_nsec", &timespec::tv_nsec);
    cls.def("__repr__", [](const timespec& self) {
       return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<snapc_t> cls(m, "snapc_t");
    cls.def(py::init<>());
    cls.def_readonly("seq", &snapc_t::seq);
    cls.def_readonly("snaps", &snapc_t::snaps);
    cls.def("__repr__", [](const snapc_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<parent_spec_t> cls(m, "parent_spec_t");
    cls.def(py::init<>());
    cls.def_readonly("pool_id", &parent_spec_t::pool_id);
    cls.def_readonly("image_id", &parent_spec_t::image_id);
    cls.def_readonly("snap_id", &parent_spec_t::snap_id);
    cls.def("__repr__", [](const parent_spec_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<parent_info_t> cls(m, "parent_info_t");
    cls.def(py::init<>());
    cls.def_readonly("spec", &parent_info_t::spec);
    cls.def_readonly("overlap", &parent_info_t::overlap);
    cls.def("__repr__", [](const parent_info_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<child_t> cls(m, "child_t");
    cls.def(py::init<>());
    cls.def_readonly("pool_id", &child_t::pool_id);
    cls.def_readonly("image_id", &child_t::image_id);
    cls.def("__repr__", [](const child_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<qos_t> cls(m, "qos_t");
    cls.def(py::init<>());
    cls.def_readonly("iops", &qos_t::iops);
    cls.def_readonly("bps", &qos_t::bps);
    cls.def("__repr__", [](const qos_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<snap_info_t> cls(m, "snap_info_t");
    cls.def(py::init<>());
    cls.def_readonly("id", &snap_info_t::id);
    cls.def_readonly("name", &snap_info_t::name);
    cls.def_readonly("snap_ns_type", &snap_info_t::snap_ns_type);
    cls.def_readonly("size", &snap_info_t::size);
    cls.def_readonly("flags", &snap_info_t::flags);
    cls.def_readonly("protection_status", &snap_info_t::protection_status);
    cls.def_readonly("timestamp", &snap_info_t::timestamp);
    cls.def("__repr__", [](const snap_info_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<snap_info_v2_t> cls(m, "snap_info_v2_t");
    cls.def(py::init<>());
    cls.def_readonly("id", &snap_info_v2_t::id);
    cls.def_readonly("name", &snap_info_v2_t::name);
    cls.def_readonly("snap_ns_type", &snap_info_v2_t::snap_ns_type);
    cls.def_readonly("size", &snap_info_v2_t::size);
    cls.def_readonly("flags", &snap_info_v2_t::flags);
    cls.def_readonly("protection_status", &snap_info_v2_t::protection_status);
    cls.def_readonly("timestamp", &snap_info_v2_t::timestamp);
    cls.def_readonly("du", &snap_info_v2_t::du);
    cls.def_readonly("dirty", &snap_info_v2_t::dirty);
    cls.def("__repr__", [](const snap_info_v2_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<image_info_t> cls(m, "image_info_t");
    cls.def(py::init<>());
    cls.def_readonly("id", &image_info_t::id);
    cls.def_readonly("name", &image_info_t::name);
    cls.def_readonly("order", &image_info_t::order);
    cls.def_readonly("size", &image_info_t::size);
    cls.def_readonly("stripe_unit", &image_info_t::stripe_unit);
    cls.def_readonly("stripe_count", &image_info_t::stripe_count);
    cls.def_readonly("features", &image_info_t::features);
    cls.def_readonly("flags", &image_info_t::flags);
    cls.def_readonly("snapc", &image_info_t::snapc);
    cls.def_readonly("snaps", &image_info_t::snaps);
    cls.def_readonly("parent", &image_info_t::parent);
    cls.def_readonly("timestamp", &image_info_t::timestamp);
    cls.def_readonly("data_pool_id", &image_info_t::data_pool_id);
    cls.def_readonly("watchers", &image_info_t::watchers);
    cls.def_readonly("qos", &image_info_t::qos);
    cls.def_readonly("du", &image_info_t::du);
    cls.def("__repr__", [](const image_info_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<image_info_v2_t> cls(m, "image_info_v2_t");
    cls.def(py::init<>());
    cls.def_readonly("id", &image_info_v2_t::id);
    cls.def_readonly("name", &image_info_v2_t::name);
    cls.def_readonly("order", &image_info_v2_t::order);
    cls.def_readonly("size", &image_info_v2_t::size);
    cls.def_readonly("stripe_unit", &image_info_v2_t::stripe_unit);
    cls.def_readonly("stripe_count", &image_info_v2_t::stripe_count);
    cls.def_readonly("features", &image_info_v2_t::features);
    cls.def_readonly("flags", &image_info_v2_t::flags);
    cls.def_readonly("snapc", &image_info_v2_t::snapc);
    cls.def_readonly("parent", &image_info_v2_t::parent);
    cls.def_readonly("timestamp", &image_info_v2_t::timestamp);
    cls.def_readonly("data_pool_id", &image_info_v2_t::data_pool_id);
    cls.def_readonly("watchers", &image_info_v2_t::watchers);
    cls.def_readonly("qos", &image_info_v2_t::qos);
    cls.def_readonly("du", &image_info_v2_t::du);
    cls.def_readonly("snaps", &image_info_v2_t::snaps);
    cls.def("__repr__", [](const image_info_v2_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  {
    py::class_<trash_info_t> cls(m, "trash_info_t");
    cls.def(py::init<>());
    cls.def_readonly("id", &trash_info_t::id);
    cls.def_readonly("name", &trash_info_t::name);
    cls.def_readonly("source", &trash_info_t::source);
    cls.def_readonly("deletion_time", &trash_info_t::deletion_time);
    cls.def_readonly("deferment_end_time", &trash_info_t::deferment_end_time);
    cls.def("__repr__", [](const trash_info_t& self) {
      return json_fmt(self).dump(json_indent);
    });
  }

  //
  // xRBD
  //
  {
    using list_du_func_t_1 = int (xRBD::*)(librados::IoCtx&,
        std::map<std::string, std::pair<image_info_t, int>>*);
    using list_du_func_t_2 = int (xRBD::*)(librados::IoCtx&,
        const std::vector<std::string>&,
        std::map<std::string, std::pair<image_info_t, int>>*);

    using list_info_func_t_1 = int (xRBD::*)(librados::IoCtx&,
        std::map<std::string, std::pair<image_info_t, int>>*);
    using list_info_func_t_2 = int (xRBD::*)(librados::IoCtx&,
        const std::vector<std::string>&,
        std::map<std::string, std::pair<image_info_t, int>>*);

    using list_info_v2_func_t_1 = int (xRBD::*)(librados::IoCtx&,
        std::map<std::string, std::pair<image_info_v2_t, int>>*);
    using list_info_v2_func_t_2 = int (xRBD::*)(librados::IoCtx&,
        const std::vector<std::string>&,
        std::map<std::string, std::pair<image_info_v2_t, int>>*);

    py::class_<xRBD> cls(m, "xRBD");
    cls.def(py::init<>());

    cls.def("get_info", &xRBD::get_info);
    cls.def("get_info_v2", &xRBD::get_info_v2);

    cls.def("list", &xRBD::list);

    cls.def("list_du", (list_du_func_t_1)&xRBD::list_du);
    cls.def("list_du", (list_du_func_t_2)&xRBD::list_du);

    cls.def("list_info", (list_info_func_t_1)&xRBD::list_info);
    cls.def("list_info", (list_info_func_t_2)&xRBD::list_info);
    cls.def("list_info_v2", (list_info_v2_func_t_1)&xRBD::list_info_v2);
    cls.def("list_info_v2", (list_info_v2_func_t_2)&xRBD::list_info_v2);

    cls.def("trash_list", &xRBD::trash_list);

    // for legacy clone v1 only
    cls.def("child_list", &xRBD::child_list);
  }

} // PYBIND11_MODULE(rbdx, m)

} // namespace rbdx
