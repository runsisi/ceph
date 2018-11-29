if(WITH_SELINUX)
add_custom_target(
  ceph.pp ALL
  COMMAND ln -sf ${CMAKE_CURRENT_SOURCE_DIR}/ceph.* .
  COMMAND make -f ${SELINUX_MAKEFILE} ceph.pp
  DEPENDS ceph.te ceph.fc ceph.if)

set(datadir ${CMAKE_INSTALL_PREFIX}/share/selinux)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/ceph.pp
  DESTINATION ${datadir}/packages)
install(FILES ceph.if
  DESTINATION ${datadir}/devel/include/contrib)
endif()

if(WITH_LIBCEPH_SELINUX)
add_custom_target(
  libceph.pp ALL
  COMMAND ln -sf ${CMAKE_CURRENT_SOURCE_DIR}/libceph.* .
  COMMAND make -f ${SELINUX_MAKEFILE} libceph.pp
  DEPENDS libceph.te)

set(datadir ${CMAKE_INSTALL_PREFIX}/share/selinux)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libceph.pp
  DESTINATION ${datadir}/packages)

if(WITH_SELINUX)
add_dependencies(libceph.pp ceph.pp)
endif()
endif()
