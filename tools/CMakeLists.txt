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

cmake_minimum_required (VERSION 2.8.12)
if (POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW) # suppress warning about mac rpath
endif ()
if (POLICY CMP0068)
  cmake_policy(SET CMP0068 NEW) # rpath settings don't change install_name
endif ()

project(ORC_TOOLS C CXX)

option(BUILD_CPP_TESTS
    "Build the googletest unit tests"
    ON)

option(TEST_VALGRIND_MEMCHECK
    "Run the test suite using valgrind --tool=memcheck"
    OFF)

option(BUILD_SHARED_LIBS
    "Should we build shared libraries instead of static ones?"
    ON)

INCLUDE(SetBuildType)
INCLUDE(SetCompilerOptions)

find_package(Protobuf REQUIRED)
find_package(HDFSPP)
find_package(ORC REQUIRED)
find_package(LZ4 REQUIRED)
find_package(Snappy REQUIRED)
find_package(Zlib REQUIRED)
find_package(Zstd REQUIRED)

include_directories (
  ${PROJECT_SOURCE_DIR}/../c++/src
  ${PROJECT_BINARY_DIR}/../orc/src
  ${PROTOBUF_INCLUDE_DIR}
  )

add_subdirectory(src)

if (BUILD_CPP_TESTS)
  enable_testing()

  add_subdirectory(test)
endif ()
