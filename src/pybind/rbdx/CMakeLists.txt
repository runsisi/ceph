distutils_add_cython_module(cython${PYTHON_VERSION}_rbdx ${CMAKE_CURRENT_SOURCE_DIR}/rbdx.cc)
add_dependencies(cython${PYTHON_VERSION}_rbdx librbd)
distutils_install_cython_module(cython${PYTHON_VERSION}_rbdx)
