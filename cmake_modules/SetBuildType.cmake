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

# Make sure that a build type is selected
if (NOT CMAKE_BUILD_TYPE)
  set (CMAKE_BUILD_TYPE "RELWITHDEBINFO")
endif ()

if (BUILD_SHARED_LIBS)
  set(LIB_TYPE "SHARED")
else ()
  set(LIB_TYPE "STATIC")
endif ()

message(STATUS "Build type: ${LIB_TYPE} ${CMAKE_BUILD_TYPE}")

if (NOT MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-O3 -g -DNDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
endif ()
