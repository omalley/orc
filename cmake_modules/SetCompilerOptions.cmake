# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Compiler specific flags
#
message(STATUS
  "compiler ${CMAKE_CXX_COMPILER_ID} version ${CMAKE_CXX_COMPILER_VERSION}")

set(_compiler_opts)
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  list (APPEND _compiler_opts  "-std=c++11"
                               "-Weverything"
                               "-Wno-c++98-compat"
                               "-Wno-missing-prototypes"
                               "-Wno-c++98-compat-pedantic"
                               "-Wno-padded"
                               "-Wno-covered-switch-default"
                               "-Wno-missing-noreturn"
                               "-Wno-unknown-pragmas"
                               "-Wno-gnu-zero-variadic-macro-arguments"
                               "-Wconversion"
                               "-Werror")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  if (CMAKE_CXX_COMPILER_VERSION STREQUAL "" OR
      CMAKE_CXX_COMPILER_VERSION VERSION_LESS "4.7")
    list (APPEND _compiler_opts "-std=c++0x")
  else ()
    list (APPEND _compiler_opts "-std=c++11")
  endif ()
  list (APPEND _compiler_opts "-Wall"
                              "-Wno-unknown-pragmas"
                              "-Wconversion"
                              "-Werror")
elseif (MSVC)
  add_definitions (-D_SCL_SECURE_NO_WARNINGS)
  add_definitions (-D_CRT_SECURE_NO_WARNINGS)
  # The POSIX name for this item is deprecated
  add_definitions (-D_CRT_NONSTDC_NO_DEPRECATE)
  list (APPEND _compiler_opts
     "-wd4521" # multiple copy constructors specified
     "-wd4146" # unary minus operator applied to unsigned type, result unsigned
     )
endif ()

foreach (_opt ${_compiler_opts})
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${_opt}")
endforeach ()
