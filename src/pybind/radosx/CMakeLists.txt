distutils_add_cython_module(cython${PYTHON_VERSION}_radosx ${CMAKE_CURRENT_SOURCE_DIR}/radosx.cc)
add_dependencies(cython${PYTHON_VERSION}_radosx librados)
distutils_install_cython_module(cython${PYTHON_VERSION}_radosx)
