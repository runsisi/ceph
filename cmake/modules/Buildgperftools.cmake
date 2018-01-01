# This module builds gperftools.
#
# It sets the following variables:
#
# GPERFTOOLS_INCLUDE_DIR - the tcmalloc include directories
# GPERFTOOLS_TCMALLOC_LIBRARY - link it to use tcmalloc

function(do_build_gperftools)
  set(configure_command
    CPPFLAGS=-I${LIBUNWIND_INCLUDE_DIR}
    <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --with-pic --with-tcmalloc-pagesize=64)
  set(build_command
    $(MAKE)
    COMMAND env -i $(MAKE) install)
  set(install_command
    "true")

  set(gperftools_root_dir "${CMAKE_BINARY_DIR}/gperftools")

  if(EXISTS "${PROJECT_SOURCE_DIR}/src/gperftools/configure")
    message(STATUS "gperftools already in src")
    set(source_dir
      SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/gperftools")
  else()
    message(STATUS "gperftools will be downloaded...")

    set(gperftools_version 2.6.3)
    set(gperftools_md5 d65c5e58931be568aac1621f2d8ad8d0)
    set(gperftools_url
      https://github.com/gperftools/gperftools/releases/download/gperftools-${gperftools_version}/gperftools-${gperftools_version}.tar.gz)
    set(source_dir
      URL ${gperftools_url}
      URL_MD5 ${gperftools_md5})
    if(CMAKE_VERSION VERSION_GREATER 3.0)
      list(APPEND source_dir DOWNLOAD_NO_PROGRESS 1)
    endif()
  endif()

  include(ExternalProject)
  ExternalProject_Add(gperftools-ext
    DEPENDS libunwind-ext
    PREFIX "${gperftools_root_dir}"
    ${source_dir}
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${configure_command}
    BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${build_command}
    INSTALL_COMMAND ${install_command})

  # force gperftools make to be called on each time
  ExternalProject_Add_Step(gperftools-ext forcebuild
    DEPENDEES configure
    DEPENDERS build
    COMMAND "true"
    ALWAYS 1)
endfunction()

macro(build_gperftools)
  if(NOT TARGET libunwind-ext)
    include(Buildlibunwind)
    build_libunwind()
  endif()

  do_build_gperftools()

  ExternalProject_Get_Property(gperftools-ext install_dir)

  set(GPERFTOOLS_INCLUDE_DIR ${install_dir}/include)
  file(MAKE_DIRECTORY ${GPERFTOOLS_INCLUDE_DIR})

  add_library(gperftools_tcmalloc STATIC IMPORTED)
  add_dependencies(gperftools_tcmalloc gperftools-ext)

  set_target_properties(gperftools_tcmalloc PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${GPERFTOOLS_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${install_dir}/lib/libtcmalloc.a")

  set(GPERFTOOLS_TCMALLOC_LIBRARY gperftools_tcmalloc ${LIBUNWIND_LIBRARY})
endmacro()
