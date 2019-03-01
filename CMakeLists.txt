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

project(ORC C CXX)

set (CMAKE_PROJECT_VERSION_MAJOR 1)
set (CMAKE_PROJECT_VERSION_MINOR 6)
set (CMAKE_PROJECT_VERSION_PATCH "0-SNAPSHOT")
set (ORC_VERSION
     ${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}.${CMAKE_PROJECT_VERSION_PATCH})

set (PROJECT_DESCRIPTION
     "A highly compressed fast columnar file format for big data")
set (PROJECT_HOMEPAGE_URL "https://orc.apache.org/")


message(STATUS "Building ORC ${ORC_VERSION}")

option(BUILD_SHARED_LIBS
       "Build shared libraries instead of static ones"
       ON)

option (BUILD_JAVA
    "Include ORC Java library in the build process"
     ON)

option (ANALYZE_JAVA
    "Run static analysis of the Java"
     OFF)

option (BUILD_LIBHDFSPP
    "Include LIBHDFSPP library in the build process"
     ON)

option(BUILD_CPP_TESTS
    "Build the googletest unit tests"
    ON)

option(BUILD_TOOLS
    "Build the tools"
    ON)

option(TEST_VALGRIND_MEMCHECK
    "Run the test suite using valgrind --tool=memcheck"
    OFF)

option(INSTALL_VENDORED_LIBS
    "Install vendored thirdparty libraries along with liborc"
    ON)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
set(STAGING_DIR "${CMAKE_BINARY_DIR}/staging")
set_directory_properties(PROPERTIES EP_BASE "${CMAKE_BINARY_DIR}/projects")
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${STAGING_DIR})
string(REPLACE ";" "|" quoted_prefix_path "${CMAKE_PREFIX_PATH}")

INCLUDE(SetBuildType)

# Set the rpath so that we find the shared libraries.
if (BUILD_SHARED_LIBS)
  if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
    set(CMAKE_INSTALL_RPATH "@loader_path/../lib")
  elseif (UNIX)
    set(CMAKE_INSTALL_RPATH "\\$ORIGIN/../lib")
  endif ()
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif ()

INCLUDE(ExternalProject)

enable_testing()

# Find or build the non-orc c++ libaries
add_subdirectory(third_party)

# We add our two modules as external projects so that they can use
# find_package to locate their dependencies, even if we are using the
# version that we build ourselves.

set (ORC_CMAKE_ARGS
     -DBUILD_CPP_TESTS=${BUILD_CPP_TESTS}
     -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS}
     -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
     -DCMAKE_INSTALL_PREFIX=${STAGING_DIR}
     -DCMAKE_INSTALL_RPATH=${CMAKE_INSTALL_RPATH}
     -DCMAKE_INSTALL_RPATH_USE_LINK_PATH=${CMAKE_INSTALL_RPATH_USE_LINK_PATH}
     -DCMAKE_MACOSX_RPATH=${CMAKE_MACOSX_RPATH}
     -DCMAKE_MODULE_PATH=${CMAKE_MODULE_PATH}
     -DCMAKE_PREFIX_PATH=${quoted_prefix_path}
     -DEXAMPLE_DIRECTORY=${CMAKE_SOURCE_DIR}/examples
     -DORC_MAJOR_MINOR=${CMAKE_PROJECT_VERSION_MAJOR}.${CMAKE_PROJECT_VERSION_MINOR}
     -DORC_VERSION=${ORC_VERSION}
    )

ExternalProject_Add (orc
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/c++
  CMAKE_ARGS ${ORC_CMAKE_ARGS}
  INSTALL_DIR ${STAGING_DIR}
  LIST_SEPARATOR |
  TEST_BEFORE_INSTALL 1
  TEST_COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
                                      --output-on-failure
  DEPENDS third_party
  )

if (BUILD_TOOLS)
  ExternalProject_Add (tools
    SOURCE_DIR ${CMAKE_SOURCE_DIR}/tools
    CMAKE_ARGS ${ORC_CMAKE_ARGS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    TEST_BEFORE_INSTALL 1
    TEST_COMMAND ${CMAKE_CTEST_COMMAND} --force-new-ctest-process
                                        --output-on-failure
    DEPENDS orc
  )
endif (BUILD_TOOLS)

if (BUILD_JAVA)
  add_subdirectory("java")
endif (BUILD_JAVA)

install(
  DIRECTORY
  ${STAGING_DIR}/bin ${STAGING_DIR}/include ${STAGING_DIR}/lib
  DESTINATION .
  OPTIONAL)

install(
  FILES LICENSE NOTICE
  DESTINATION "share/doc/orc")

set(CPACK_GENERATOR "TGZ")
set(CPACK_PACKAGE_VENDOR "Apache")
set(CPACK_PACKAGE_CONTACT "Apache ORC <dev@orc.apache.org>")
INCLUDE (CPack)
