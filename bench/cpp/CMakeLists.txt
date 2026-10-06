cmake_minimum_required(VERSION 3.16)
project(aeron_bench_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT DEFINED AERON_ROOT)
    set(AERON_ROOT "$ENV{AERON_ROOT}")
    if(AERON_ROOT STREQUAL "")
        set(AERON_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../../aeron")
    endif()
endif()

set(AERON_BUILD_DIR "${AERON_ROOT}/build")
set(AERON_CPP_INCLUDE_DIR "${AERON_ROOT}/aeron-client/src/main/cpp_wrapper")
set(AERON_C_INCLUDE_DIR "${AERON_ROOT}/aeron-client/src/main/c")

set(AERON_SHARED_LIB "${AERON_BUILD_DIR}/lib/libaeron.${CMAKE_SHARED_LIBRARY_SUFFIX}")
set(AERON_STATIC_LIB "${AERON_BUILD_DIR}/lib/libaeron_static.a")

if(EXISTS "${AERON_SHARED_LIB}")
    set(AERON_LIB "${AERON_SHARED_LIB}")
elseif(EXISTS "${AERON_STATIC_LIB}")
    set(AERON_LIB "${AERON_STATIC_LIB}")
else()
    message(FATAL_ERROR "Aeron library not found in ${AERON_BUILD_DIR}/lib. "
        "Run: cmake -S ${AERON_ROOT} -B ${AERON_BUILD_DIR} && "
        "cmake --build ${AERON_BUILD_DIR} -j 8")
endif()

add_executable(bench_cpp bench.cpp)
target_include_directories(bench_cpp PRIVATE
    "${AERON_CPP_INCLUDE_DIR}"
    "${AERON_C_INCLUDE_DIR}")
target_link_libraries(bench_cpp PRIVATE "${AERON_LIB}")

if(APPLE)
    find_library(CF_COREFOUNDATION CoreFoundation)
    target_link_libraries(bench_cpp PRIVATE "${CF_COREFOUNDATION}")
endif()

set_target_properties(bench_cpp PROPERTIES
    BUILD_RPATH "${AERON_BUILD_DIR}/lib"
    INSTALL_RPATH "${AERON_BUILD_DIR}/lib")

add_executable(bench_cpp_multiproc bench_multiproc.cpp)
target_include_directories(bench_cpp_multiproc PRIVATE
    "${AERON_CPP_INCLUDE_DIR}"
    "${AERON_C_INCLUDE_DIR}")
target_link_libraries(bench_cpp_multiproc PRIVATE "${AERON_LIB}" pthread)

if(APPLE)
    target_link_libraries(bench_cpp_multiproc PRIVATE "${CF_COREFOUNDATION}")
endif()

set_target_properties(bench_cpp_multiproc PROPERTIES
    BUILD_RPATH "${AERON_BUILD_DIR}/lib"
    INSTALL_RPATH "${AERON_BUILD_DIR}/lib")

add_executable(bench_cpp_rpc rpc.cpp)
target_include_directories(bench_cpp_rpc PRIVATE
    "${AERON_CPP_INCLUDE_DIR}"
    "${AERON_C_INCLUDE_DIR}")
target_link_libraries(bench_cpp_rpc PRIVATE "${AERON_LIB}")

if(APPLE)
    target_link_libraries(bench_cpp_rpc PRIVATE "${CF_COREFOUNDATION}")
endif()

set_target_properties(bench_cpp_rpc PROPERTIES
    BUILD_RPATH "${AERON_BUILD_DIR}/lib"
    INSTALL_RPATH "${AERON_BUILD_DIR}/lib")
