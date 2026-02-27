cmake_minimum_required(VERSION 3.16)
project(zsv_jni LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Build zsv using ExternalProject_Add
include(ExternalProject)

ExternalProject_Add(
  zsv_external
  SOURCE_DIR ${CMAKE_SOURCE_DIR}/third_party/zsv
  CONFIGURE_COMMAND sh -c "cd ${CMAKE_SOURCE_DIR}/third_party/zsv && ./configure && cd src && make build"
  BUILD_COMMAND ""
  INSTALL_COMMAND ""
  BUILD_BYPRODUCTS ${CMAKE_SOURCE_DIR}/third_party/zsv/build/Linux/rel/gcc/lib/libzsv.a
)

# NOTE: The following path is for Linux/gcc. Update if building on another platform or with another compiler.
add_library(zsv STATIC IMPORTED)
set_target_properties(zsv PROPERTIES
  IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/third_party/zsv/build/Linux/rel/gcc/lib/libzsv.a
  INTERFACE_INCLUDE_DIRECTORIES ${CMAKE_SOURCE_DIR}/third_party/zsv/include
)

add_dependencies(zsv zsv_external)

# --- JNI library ---
add_library(zsv_jni SHARED
  jni/zsv_jni.cpp
)


# fast_float headers (header-only)
target_include_directories(zsv_jni PRIVATE
  ${CMAKE_SOURCE_DIR}/third_party/fast_float/include
)


target_link_libraries(zsv_jni PRIVATE zsv)

# --- JNI headers via JAVA_HOME ---
if(NOT DEFINED ENV{JAVA_HOME})
  message(FATAL_ERROR "JAVA_HOME is not set. Please export JAVA_HOME.")
endif()
set(JAVA_HOME $ENV{JAVA_HOME})

target_include_directories(zsv_jni PRIVATE
  ${JAVA_HOME}/include
  ${JAVA_HOME}/include/linux
)

target_compile_definitions(zsv_jni PRIVATE restrict=__restrict__)

# --- io_uring (optional but strongly recommended) ---
find_library(URING_LIB uring)
find_path(URING_INCLUDE liburing.h)
if(URING_LIB AND URING_INCLUDE)
  message(STATUS "Found liburing: ${URING_LIB}")
  target_link_libraries(zsv_jni PRIVATE ${URING_LIB})
  target_include_directories(zsv_jni PRIVATE ${URING_INCLUDE})
  target_compile_definitions(zsv_jni PRIVATE HAVE_URING=1)
else()
  message(WARNING "liburing not found — io_uring batch reader will be unavailable at runtime. "
                  "Install with: sudo apt-get install liburing-dev")
endif()
