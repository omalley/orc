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

# Append str to each item in the given list.
# replaceable with list(TRANSFORM var APPEND ${str}) once we have cmake 3.10.
function (appendToEach var str)
  set(_result)
  foreach (_elem ${${var}})
    list(APPEND _result "${_elem}${str}")
  endforeach ()
  set(${var} ${_result} PARENT_SCOPE)
endfunction (appendToEach)

# Given a variable and a list of capitialized names of libraries, add their
# directory to the variable.
function (addToRPath var)
  set(_result "${${var}}")
  message(STATUS "OOM starting with ${_result}")
  foreach (_lib ${ARGN})
    message(STATUS "OOM checking ${_lib} = ${${_lib}_FOUND}")
    if (${_lib}_FOUND)
      foreach (_path ${${_lib}_LIBRARIES})
        get_filename_component(_dir ${_path} DIRECTORY)
        message(STATUS "OOM checking path ${_dir}")
        list (FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${_dir}" _is_sys)
        if (NOT "${_is_sys}" STREQUAL "-1")
          message(STATUS "OOM adding directory ${_dir}")
          list(APPEND _result ${_dir})
        endif ()
      endforeach ()
    endif()
  endforeach()
  list(REMOVE_DUPLICATES _result)
  message(STATUS "OOM final result = ${_result}")
  set(${var} ${_result} PARENT_SCOPE)
endfunction (addToRPath)