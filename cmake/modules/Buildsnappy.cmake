# This module builds snappy.
#
# It sets the following variables:
#
# SNAPPY_INCLUDE_DIR - the snappy include directories
# SNAPPY_LIBRARY - link it to use snappy
# SNAPPY_LIBRARIES - link it to use snappy

function(do_build_snappy)
  set(SNAPPY_CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON)
  list(APPEND SNAPPY_CMAKE_ARGS -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER})
  list(APPEND SNAPPY_CMAKE_ARGS -DCMAKE_AR=${CMAKE_AR})
  list(APPEND SNAPPY_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})
  list(APPEND SNAPPY_CMAKE_ARGS -DSNAPPY_BUILD_TESTS=OFF)
  list(APPEND SNAPPY_CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>)
  list(APPEND SNAPPY_CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR=lib)

  set(build_command
    $(MAKE) snappy
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

    set(snappy_version 1.1.7)
    set(snappy_md5 ee9086291c9ae8deb4dac5e0b85bf54a)
    set(snappy_url
      https://github.com/google/snappy/archive/${snappy_version}.tar.gz)
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
    CMAKE_ARGS ${SNAPPY_CMAKE_ARGS}
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

  set(SNAPPY_INCLUDE_DIR ${install_dir}/include)
  file(MAKE_DIRECTORY ${SNAPPY_INCLUDE_DIR})

  add_library(snappy STATIC IMPORTED)
  add_dependencies(snappy snappy-ext)

  set_target_properties(snappy PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${SNAPPY_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "CXX"
    IMPORTED_LOCATION "${install_dir}/lib/libsnappy.a")

  set(SNAPPY_LIBRARY snappy)
  set(SNAPPY_LIBRARIES snappy)
endmacro()
