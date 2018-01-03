# This module builds snappy.
#
# It sets the following variables:
#
# SNAPPY_INCLUDE_DIRS - the snappy include directories
# SNAPPY_LIBRARIES - link it to use snappy

function(do_build_snappy)
  set(configure_command
    <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --with-pic)
  set(build_command
    $(MAKE)
    COMMAND env -i $(MAKE) install)
  set(install_command
    "true")

  set(snappy_root_dir "${CMAKE_BINARY_DIR}/snappy")

  if(EXISTS "${PROJECT_SOURCE_DIR}/src/snappy/snappy.h")
    message(STATUS "snappy already in src")
    set(source_dir
      SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/snappy")
  else()
    message(STATUS "snappy will be downloaded...")

    set(snappy_version 1.1.4)
    set(snappy_md5 c328993b68afe3e5bd87c8ea9bdeb028)
    set(snappy_url
      https://github.com/google/snappy/releases/download/${snappy_version}/snappy-${snappy_version}.tar.gz)
    set(source_dir
      URL ${snappy_url}
      URL_MD5 ${snappy_md5})
    if(CMAKE_VERSION VERSION_GREATER 3.0)
      list(APPEND source_dir DOWNLOAD_NO_PROGRESS 1)
    endif()
  endif()

  include(ExternalProject)
  ExternalProject_Add(snappy-ext
    PREFIX "${snappy_root_dir}"
    ${source_dir}
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND CC=${CMAKE_C_COMPILER} CXX=${CMAKE_CXX_COMPILER} ${configure_command}
    BUILD_COMMAND ${build_command}
    INSTALL_COMMAND ${install_command})

  # force snappy make to be called on each time
  ExternalProject_Add_Step(snappy-ext forcebuild
    DEPENDEES configure
    DEPENDERS build
    COMMAND "true"
    ALWAYS 1)
endfunction()

macro(build_snappy)
  do_build_snappy()

  ExternalProject_Get_Property(snappy-ext install_dir)

  set(SNAPPY_INCLUDE_DIRS ${install_dir}/include)
  file(MAKE_DIRECTORY ${SNAPPY_INCLUDE_DIRS})

  add_library(snappy STATIC IMPORTED)
  add_dependencies(snappy snappy-ext)

  set_target_properties(snappy PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${SNAPPY_INCLUDE_DIRS}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${install_dir}/lib/libsnappy.a")

  set(SNAPPY_LIBRARIES snappy)
endmacro()
