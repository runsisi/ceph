# This module builds libunwind.
#
# It sets the following variables:
#
# LIBUNWIND_INCLUDE_DIR - the libunwind include directories
# LIBUNWIND_LIBRARY - link it to use libunwind

function(do_build_libunwind)
  set(configure_command
    <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --with-pic --disable-minidebuginfo)
  set(build_command
    $(MAKE)
    COMMAND $(MAKE) install)
  set(install_command
    "true")

  set(libunwind_root_dir "${CMAKE_BINARY_DIR}/libunwind")

  if(EXISTS "${PROJECT_SOURCE_DIR}/src/libunwind/configure")
    message(STATUS "libunwind already in src")
    set(source_dir
      SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/libunwind")
  else()
    message(STATUS "libunwind will be downloaded...")

    set(libunwind_version 1.2.1)
    set(libunwind_md5 06ba9e60d92fd6f55cd9dadb084df19e)
    set(libunwind_url
      https://github.com/libunwind/libunwind/releases/download/v${libunwind_version}/libunwind-${libunwind_version}.tar.gz)
    set(source_dir
      URL ${libunwind_url}
      URL_MD5 ${libunwind_md5})
    if(CMAKE_VERSION VERSION_GREATER 3.0)
      list(APPEND source_dir DOWNLOAD_NO_PROGRESS 1)
    endif()
  endif()

  include(ExternalProject)
  ExternalProject_Add(libunwind-ext
    PREFIX "${libunwind_root_dir}"
    ${source_dir}
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${configure_command}
    BUILD_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${build_command}
    INSTALL_COMMAND ${install_command})

  # force libunwind make to be called on each time
  ExternalProject_Add_Step(libunwind-ext forcebuild
    DEPENDEES configure
    DEPENDERS build
    COMMAND "true"
    ALWAYS 1)
endfunction()

macro(build_libunwind)
  do_build_libunwind()

  ExternalProject_Get_Property(libunwind-ext install_dir)

  set(LIBUNWIND_INCLUDE_DIR ${install_dir}/include)
  file(MAKE_DIRECTORY ${LIBUNWIND_INCLUDE_DIR})

  add_library(libunwind STATIC IMPORTED)
  add_dependencies(libunwind libunwind-ext)

  set_target_properties(libunwind PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${LIBUNWIND_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${install_dir}/lib/libunwind.a")

  set(LIBUNWIND_LIBRARY libunwind)
endmacro()
