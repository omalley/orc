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

set(LZ4_VERSION "1.7.5")
set(SNAPPY_VERSION "1.1.7")
set(ZLIB_VERSION "1.2.11")
set(GTEST_VERSION "1.8.1")
set(PROTOBUF_VERSION "3.5.1")
set(ZSTD_VERSION "1.3.5")

# Ok, the fact that we want to use the native package or build new instances
# makes the dependencies tricky, precisely because you have different targets
# depending on whether you needed to build it. I've divided the packages in
# to stages so that all of the dependencies are only between stages. If new
# packages are added, add them to the first stage where every package they
# depend on is already built.

# Stage 1:
#   * gtest
#   * lz4
#   * protobuf
#   * snappy
#   * zlib
#   * zstd

# Stage 2:
#   * hdfspp

# We define the third_party targets to control the dependencies.
add_custom_target(third_party_stage1)
add_custom_target(third_party_stage2 DEPENDS third_party_stage1)
add_custom_target(third_party DEPENDS third_party_stage2)

# zstd requires us to add the threads
FIND_PACKAGE(Threads REQUIRED)

# Set the most common options that we want for the dependencies
set(THIRDPARTY_LOG_OPTIONS LOG_CONFIGURE 1
                           LOG_BUILD 1
                           LOG_INSTALL 1
                           LOG_DOWNLOAD 1)
set(THIRDPARTY_CMAKE_OPTIONS
  -DBUILD_SHARED_LIBS=${BUILD_SHARED_LIBS}
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DCMAKE_INSTALL_LIBDIR=lib
  -DCMAKE_INSTALL_PREFIX=${STAGING_DIR}
  -DCMAKE_INSTALL_RPATH=${CMAKE_INSTALL_RPATH}
  -DCMAKE_MACOSX_RPATH=${CMAKE_MACOSX_RPATH}
  -DCMAKE_PREFIX_PATH=${quoted_prefix_path}
  )

set(THIRDPARTY_CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}")
if (CMAKE_GENERATOR_TOOLSET)
  list(APPEND THIRDPARTY_CONFIGURE_COMMAND -T "${CMAKE_GENERATOR_TOOLSET}")
endif ()

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

# ----------------------------------------------------------------------
# Snappy

find_package(Snappy ${SNAPPY_VERSION})

if (NOT SNAPPY_FOUND)
  set (SNAPPY_CMAKE_ARGS ${THIRDPARTY_CMAKE_OPTIONS})
  list (APPEND SNAPPY_CMAKE_ARGS -DSNAPPY_BUILD_TESTS=OFF)
  ExternalProject_Add (snappy_project
    URL "https://github.com/google/snappy/archive/${SNAPPY_VERSION}.tar.gz"
    CMAKE_ARGS ${SNAPPY_CMAKE_ARGS}
    ${THIRDPARTY_LOG_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
  )
  add_dependencies(third_party_stage1 snappy_project)
endif ()

# ----------------------------------------------------------------------
# ZLIB

find_package(Zlib ${ZLIB_VERSION})

if (NOT ZLIB_FOUND)
  ExternalProject_Add (zlib_project
    URL "http://zlib.net/fossils/zlib-${ZLIB_VERSION}.tar.gz"
    CMAKE_ARGS ${THIRDPARTY_CMAKE_OPTIONS}
    ${THIRDPARTY_LOG_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
  )
  add_dependencies(third_party_stage1 zlib_project)
endif ()

# ----------------------------------------------------------------------
# Zstd

find_package(Zstd ${ZSTD_VERSION})

if (NOT ZSTD_FOUND)
  if (CMAKE_VERSION VERSION_GREATER "3.7")
    set(ZSTD_CONFIGURE
        SOURCE_SUBDIR "build/cmake" CMAKE_ARGS ${THIRDPARTY_CMAKE_OPTIONS})
  else()
    set(ZSTD_CONFIGURE
        CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}"
        ${THIRDPARTY_CMAKE_OPTIONS}
        "${CMAKE_CURRENT_BINARY_DIR}/../projects/Source/zstd_project/build/cmake")
  endif()
  ExternalProject_Add(zstd_project
    URL "https://github.com/facebook/zstd/archive/v${ZSTD_VERSION}.tar.gz"
    ${ZSTD_CONFIGURE}
    ${THIRDPARTY_LOG_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
    PATCH_COMMAND patch -u -N -p0 < ${CMAKE_CURRENT_SOURCE_DIR}/gtest.patch
  )
  add_dependencies(third_party_stage1 zstd_project)
endif ()

# ----------------------------------------------------------------------
# LZ4

find_package(LZ4 ${LZ4_VERSION})

if (NOT LZ4_FOUND)
  if (CMAKE_VERSION VERSION_GREATER "3.7")
    set(LZ4_CONFIGURE SOURCE_SUBDIR "contrib/cmake_unofficial" CMAKE_ARGS
                      ${THIRDPARTY_CMAKE_OPTIONS})
  else()
    set(LZ4_CONFIGURE CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}"
        ${THIRDPARTY_CMAKE_OPTIONS}
        "${CMAKE_CURRENT_BINARY_DIR}/../projects/Source/lz4_project/contrib/cmake_unofficial")
  endif()
  ExternalProject_Add(lz4_project
    URL "https://github.com/lz4/lz4/archive/v${LZ4_VERSION}.tar.gz"
    ${LZ4_CONFIGURE}
    ${THIRDPARTY_LOG_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
  )
  add_dependencies(third_party_stage1 lz4_project)
endif ()

# ----------------------------------------------------------------------
# IANA - Time Zone Database

if (WIN32)
  ExternalProject_Add(tzdata_project
    URL "ftp://cygwin.osuosl.org/pub/cygwin/noarch/release/tzdata/tzdata-2018c-1.tar.xz"
    URL_HASH MD5=F69FCA5C906FAFF02462D3D06F28267C
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
  )
  ExternalProject_Get_Property(tzdata_project SOURCE_DIR)
  set(TZDATA_DIR ${SOURCE_DIR}/share/zoneinfo)
  add_dependencies(third_party_stage1 tzdata_project)
endif ()

# ----------------------------------------------------------------------
# GoogleTest (gtest now includes gmock)

find_package(GTest ${GTEST_VERSION})

if (NOT GTEST_FOUND)
  set(GTEST_CMAKE_ARGS ${THIRDPARTY_CMAKE_OPTIONS})
  if(APPLE)
    set(GTEST_CMAKE_CXX_FLAGS " -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
  else()
    set(GTEST_CMAKE_CXX_FLAGS "")
  endif()

  list(APPEND GTEST_CMAKE_ARGS
              -Dgtest_force_shared_crt=ON
              -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})
  ExternalProject_Add (gtest_project
    URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
    CMAKE_ARGS "${GTEST_CMAKE_ARGS}"
    ${THIRDPARTY_LOG_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
  )
  add_dependencies(third_party_stage1 gtest_project)
endif ()

# ----------------------------------------------------------------------
# Protobuf

find_package(Protobuf ${PROTOBUF_VERSION})

if (NOT PROTOBUF_FOUND)
  set(PROTOBUF_CMAKE_ARGS ${THIRDPARTY_CMAKE_OPTIONS})
  list(APPEND PROTOBUF_CMAKE_ARGS -Dprotobuf_BUILD_TESTS=OFF
                                  )
  if (MSVC)
    list(APPEND PROTOBUF_CMAKE_ARGS -Dprotobuf_MSVC_STATIC_RUNTIME=OFF
                                    -Dprotobuf_DEBUG_POSTFIX=)
  endif ()

  if (CMAKE_VERSION VERSION_GREATER "3.7")
    set(PROTOBUF_CONFIGURE SOURCE_SUBDIR "cmake" CMAKE_ARGS ${PROTOBUF_CMAKE_ARGS})
  else()
    set(PROTOBUF_CONFIGURE
        CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}"
        ${PROTOBUF_CMAKE_ARGS}
        "${CMAKE_CURRENT_BINARY_DIR}/../projects/Source/protobuf_project/cmake")
  endif()

  ExternalProject_Add(protobuf_project
    URL "https://github.com/google/protobuf/archive/v${PROTOBUF_VERSION}.tar.gz"
    ${PROTOBUF_CONFIGURE}
    ${THIRDPARTY_LOG_OPTIONS}
    INSTALL_DIR ${STAGING_DIR}
    LIST_SEPARATOR |
  )
  add_dependencies(third_party_stage1 protobuf_project)
endif ()

# ----------------------------------------------------------------------
# LIBHDFSPP

if(BUILD_LIBHDFSPP)
  INCLUDE(CheckCXXSourceCompiles)
  INCLUDE(SetCompilerOptions)

  CHECK_CXX_SOURCE_COMPILES("
    #include <thread>
    int main(void) {
      thread_local int s;
      return s;
    }"
    CXX_HAS_THREAD_LOCAL
  )

  if (NOT CXX_HAS_THREAD_LOCAL)
    message(WARNING
      "WARNING: Hdfs++ library was not built because the required feature "
      "thread_local storage is not supported by your compiler. Known "
      "compilers that support this feature: GCC, Visual Studio, Clang "
      " (community version), Clang (for iOS 9 and later), Clang (for Xcode "
      "8 and later)")
  else ()
    find_package(HDFSPP)
    if (NOT HDFSPP_FOUND)
      set (HDFSPP_CMAKE_ARGS ${THIRDPARTY_CMAKE_OPTIONS})
      list (APPEND HDFSPP_CMAKE_ARGS
                   -DHDFSPP_LIBRARY_ONLY=ON)
      set(HDFSPP_ROOT
        hadoop-hdfs-project/hadoop-hdfs-native-client/src/main/native/libhdfspp)
      if (CMAKE_VERSION VERSION_GREATER "3.7")
        set(HDFSPP_CONFIGURE
          SOURCE_SUBDIR ${HDFSPP_ROOT} CMAKE_ARGS ${HDFSPP_CMAKE_ARGS})
      else()
        set(HDFSPP_CONFIGURE
          CONFIGURE_COMMAND "${THIRDPARTY_CONFIGURE_COMMAND}"
          ${HDFSPP_CMAKE_ARGS}
          "${CMAKE_CURRENT_BINARY_DIR}/../projects/Source/hdfspp_project/${HDFSPP_ROOT}")
      endif()
      ExternalProject_Add (hdfspp_project
        URL https://github.com/omalley/hadoop/archive/hdfs-14244.zip
        DEPENDS third_party_stage1
        INSTALL_DIR ${STAGING_DIR}
        ${HDFSPP_CONFIGURE}
        LIST_SEPARATOR |
      )
      add_dependencies(third_party_stage2 hdfspp_project)
    endif (NOT HDFSPP_FOUND)
  endif ()
endif ()
