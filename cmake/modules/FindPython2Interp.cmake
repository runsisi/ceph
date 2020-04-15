#.rst:
# FindPython2Interp
# ----------------
#
# Find python interpreter
#
# This module finds if Python interpreter is installed and determines
# where the executables are.  This code sets the following variables:
#
# ::
#
#   PYTHON2INTERP_FOUND         - Was the Python executable found
#   PYTHON2_EXECUTABLE          - path to the Python interpreter
#
#
#
# ::
#
#   PYTHON2_VERSION_STRING      - Python version found e.g. 2.5.2
#   PYTHON2_VERSION_MAJOR       - Python major version found e.g. 2
#   PYTHON2_VERSION_MINOR       - Python minor version found e.g. 5
#   PYTHON2_VERSION_PATCH       - Python patch version found e.g. 2
#
#
#
# The Python2_ADDITIONAL_VERSIONS variable can be used to specify a list
# of version numbers that should be taken into account when searching
# for Python.  You need to set this variable before calling
# find_package(Python2Interp).
#
# If calling both ``find_package(Python2Interp)`` and
# ``find_package(Python2Libs)``, call ``find_package(Python2Interp)`` first to
# get the currently active Python version by default with a consistent version
# of PYTHON2_LIBRARIES.

#=============================================================================
# Copyright 2005-2010 Kitware, Inc.
# Copyright 2011 Bjoern Ricks <bjoern.ricks@gmail.com>
# Copyright 2012 Rolf Eike Beer <eike@sf-mail.de>
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# * Redistributions of source code must retain the above copyright
#   notice, this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright
#   notice, this list of conditions and the following disclaimer in the
#   documentation and/or other materials provided with the distribution.
#
# * Neither the names of Kitware, Inc., the Insight Software Consortium,
#   nor the names of their contributors may be used to endorse or promote
#   products derived from this software without specific prior written
#   permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#=============================================================================

unset(_Python2_NAMES)

set(_PYTHON2_VERSIONS 2.7 2.6 2.5 2.4 2.3 2.2 2.1 2.0)

if(Python2Interp_FIND_VERSION)
    if(Python2Interp_FIND_VERSION_COUNT GREATER 1)
        set(_PYTHON2_FIND_MAJ_MIN "${Python2Interp_FIND_VERSION_MAJOR}.${Python2Interp_FIND_VERSION_MINOR}")
        list(APPEND _Python2_NAMES
             python${_PYTHON2_FIND_MAJ_MIN}
             python${Python2Interp_FIND_VERSION_MAJOR})
        unset(_PYTHON2_FIND_OTHER_VERSIONS)
        if(NOT Python2Interp_FIND_VERSION_EXACT)
            foreach(_PYTHON2_V ${_PYTHON${Python2Interp_FIND_VERSION_MAJOR}_VERSIONS})
                if(NOT _PYTHON2_V VERSION_LESS _PYTHON2_FIND_MAJ_MIN)
                    list(APPEND _PYTHON2_FIND_OTHER_VERSIONS ${_PYTHON2_V})
                endif()
             endforeach()
        endif()
        unset(_PYTHON2_FIND_MAJ_MIN)
    else()
        list(APPEND _Python2_NAMES python${Python2Interp_FIND_VERSION_MAJOR})
        set(_PYTHON2_FIND_OTHER_VERSIONS ${_PYTHON${Python2Interp_FIND_VERSION_MAJOR}_VERSIONS})
    endif()
else()
    set(_PYTHON2_FIND_OTHER_VERSIONS ${_PYTHON2_VERSIONS})
endif()
find_program(PYTHON2_EXECUTABLE NAMES ${_Python2_NAMES})

# Set up the versions we know about, in the order we will search. Always add
# the user supplied additional versions to the front.
set(_Python2_VERSIONS ${Python2_ADDITIONAL_VERSIONS})
# If FindPython2Interp has already found the major and minor version,
# insert that version next to get consistent versions of the interpreter and
# library.
if(DEFINED PYTHON2LIBS_VERSION_STRING)
  string(REPLACE "." ";" _PYTHON2LIBS_VERSION "${PYTHON2LIBS_VERSION_STRING}")
  list(GET _PYTHON2LIBS_VERSION 0 _PYTHON2LIBS_VERSION_MAJOR)
  list(GET _PYTHON2LIBS_VERSION 1 _PYTHON2LIBS_VERSION_MINOR)
  list(APPEND _Python2_VERSIONS ${_PYTHON2LIBS_VERSION_MAJOR}.${_PYTHON2LIBS_VERSION_MINOR})
endif()
# Search for the current active python version first
list(APPEND _Python2_VERSIONS ";")
list(APPEND _Python2_VERSIONS ${_PYTHON2_FIND_OTHER_VERSIONS})

unset(_PYTHON2_FIND_OTHER_VERSIONS)
unset(_PYTHON2_VERSIONS)

# Search for newest python version if python executable isn't found
if(NOT PYTHON2_EXECUTABLE)
    foreach(_CURRENT_VERSION IN LISTS _Python2_VERSIONS)
      set(_Python2_NAMES python${_CURRENT_VERSION})
      if(WIN32)
        list(APPEND _Python2_NAMES python)
      endif()
      find_program(PYTHON2_EXECUTABLE
        NAMES ${_Python2_NAMES}
        PATHS [HKEY_LOCAL_MACHINE\\SOFTWARE\\Python\\PythonCore\\${_CURRENT_VERSION}\\InstallPath]
        )
    endforeach()
endif()

# determine python version string
if(PYTHON2_EXECUTABLE)
    execute_process(COMMAND "${PYTHON2_EXECUTABLE}" -c
                            "import sys; sys.stdout.write(';'.join([str(x) for x in sys.version_info[:3]]))"
                    OUTPUT_VARIABLE _VERSION)
    string(REPLACE ";" "." PYTHON2_VERSION_STRING "${_VERSION}")
    list(GET _VERSION 0 PYTHON2_VERSION_MAJOR)
    list(GET _VERSION 1 PYTHON2_VERSION_MINOR)
    list(GET _VERSION 2 PYTHON2_VERSION_PATCH)
    unset(_VERSION)
endif()

# handle the QUIETLY and REQUIRED arguments and set PYTHON2INTERP_FOUND to TRUE if
# all listed variables are TRUE
include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(Python2Interp REQUIRED_VARS PYTHON2_EXECUTABLE VERSION_VAR PYTHON2_VERSION_STRING)

mark_as_advanced(PYTHON2_EXECUTABLE)
