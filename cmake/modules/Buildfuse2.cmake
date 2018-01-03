# This module builds fuse.
#
# It sets the following variables:
#
# FUSE_INCLUDE_DIRS - the fuse include directories
# FUSE_LIBRARIES - link it to use fuse

function(do_build_fuse)
  set(patch_command
    patch -p1 < ${CMAKE_SOURCE_DIR}/third-patch/libfuse2.pdiff)
  set(configure_command
    <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --with-pic)
  set(build_command
    $(MAKE)
    COMMAND env -i $(MAKE) install)
  set(install_command
    "true")

  set(fuse_root_dir "${CMAKE_BINARY_DIR}/libfuse")

  if(EXISTS "${PROJECT_SOURCE_DIR}/src/libfuse/include/fuse.h")
    message(STATUS "fuse already in src")
    set(source_dir
      SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/libfuse")
  else()
    message(STATUS "libfuse will be downloaded...")

    set(fuse_version 2.9.7)
    set(fuse_md5 9bd4ce8184745fd3d000ca2692adacdb)
    set(fuse_url
      https://github.com/libfuse/libfuse/releases/download/fuse-${fuse_version}/fuse-${fuse_version}.tar.gz)
    set(source_dir
      URL ${fuse_url}
      URL_MD5 ${fuse_md5})
    if(CMAKE_VERSION VERSION_GREATER 3.0)
      list(APPEND source_dir DOWNLOAD_NO_PROGRESS 1)
    endif()
  endif()

  include(ExternalProject)
  ExternalProject_Add(fuse-ext
    PREFIX "${fuse_root_dir}"
    ${source_dir}
    BUILD_IN_SOURCE 1
    PATCH_COMMAND ${patch_command}
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${configure_command}
    BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${build_command}
    INSTALL_COMMAND ${install_command})

  # force fuse make to be called on each time
  ExternalProject_Add_Step(fuse-ext forcebuild
    DEPENDEES configure
    DEPENDERS build
    COMMAND "true"
    ALWAYS 1)
endfunction()

macro(build_fuse)
  do_build_fuse()

  ExternalProject_Get_Property(fuse-ext install_dir)

  set(FUSE_INCLUDE_DIRS ${install_dir}/include/fuse)
  file(MAKE_DIRECTORY ${FUSE_INCLUDE_DIRS})

  add_library(fuse STATIC IMPORTED)
  add_dependencies(fuse fuse-ext)

  set_target_properties(fuse PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${FUSE_INCLUDE_DIRS}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${install_dir}/lib/libfuse.a")

  set(FUSE_LIBRARIES fuse pthread dl)
endmacro()
