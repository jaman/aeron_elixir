cmake_minimum_required(VERSION 3.16)
project(aeron_bench_c C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(NOT DEFINED AERON_ROOT)
    set(AERON_ROOT "$ENV{AERON_ROOT}")
    if(AERON_ROOT STREQUAL "")
        set(AERON_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../../aeron")
    endif()
endif()

set(AERON_BUILD_DIR "${AERON_ROOT}/build")
set(AERON_INCLUDE_DIR "${AERON_ROOT}/aeron-client/src/main/c")

set(AERON_SHARED_LIB "${AERON_BUILD_DIR}/lib/libaeron.${CMAKE_SHARED_LIBRARY_SUFFIX}")
set(AERON_STATIC_LIB "${AERON_BUILD_DIR}/lib/libaeron_static.a")

if(EXISTS "${AERON_SHARED_LIB}")
    set(AERON_LIB "${AERON_SHARED_LIB}")
    set(AERON_LIB_TYPE SHARED)
elseif(EXISTS "${AERON_STATIC_LIB}")
    set(AERON_LIB "${AERON_STATIC_LIB}")
    set(AERON_LIB_TYPE STATIC)
else()
    message(FATAL_ERROR "Aeron library not found in ${AERON_BUILD_DIR}/lib. "
        "Run: cmake -S ${AERON_ROOT} -B ${AERON_BUILD_DIR} && "
        "cmake --build ${AERON_BUILD_DIR} -j 8")
endif()

add_executable(bench_c bench.c)
target_include_directories(bench_c PRIVATE "${AERON_INCLUDE_DIR}")
target_link_libraries(bench_c PRIVATE "${AERON_LIB}")

if(APPLE)
    find_library(CF_COREFOUNDATION CoreFoundation)
    target_link_libraries(bench_c PRIVATE "${CF_COREFOUNDATION}")
endif()

if(AERON_LIB_TYPE STREQUAL "SHARED")
    set_target_properties(bench_c PROPERTIES
        BUILD_RPATH "${AERON_BUILD_DIR}/lib"
        INSTALL_RPATH "${AERON_BUILD_DIR}/lib")
endif()

add_executable(bench_c_multiproc bench_multiproc.c)
target_include_directories(bench_c_multiproc PRIVATE "${AERON_INCLUDE_DIR}")
target_link_libraries(bench_c_multiproc PRIVATE "${AERON_LIB}" pthread)

if(APPLE)
    target_link_libraries(bench_c_multiproc PRIVATE "${CF_COREFOUNDATION}")
endif()

if(AERON_LIB_TYPE STREQUAL "SHARED")
    set_target_properties(bench_c_multiproc PROPERTIES
        BUILD_RPATH "${AERON_BUILD_DIR}/lib"
        INSTALL_RPATH "${AERON_BUILD_DIR}/lib")
endif()

add_executable(bench_c_rpc rpc.c)
target_include_directories(bench_c_rpc PRIVATE "${AERON_INCLUDE_DIR}")
target_link_libraries(bench_c_rpc PRIVATE "${AERON_LIB}")

if(APPLE)
    target_link_libraries(bench_c_rpc PRIVATE "${CF_COREFOUNDATION}")
endif()

if(AERON_LIB_TYPE STREQUAL "SHARED")
    set_target_properties(bench_c_rpc PROPERTIES
        BUILD_RPATH "${AERON_BUILD_DIR}/lib"
        INSTALL_RPATH "${AERON_BUILD_DIR}/lib")
endif()

add_executable(harness_consumer_c ../harness/consumer.c)
target_include_directories(harness_consumer_c PRIVATE "${AERON_INCLUDE_DIR}")
target_link_libraries(harness_consumer_c PRIVATE "${AERON_LIB}")

if(APPLE)
    target_link_libraries(harness_consumer_c PRIVATE "${CF_COREFOUNDATION}")
endif()

if(AERON_LIB_TYPE STREQUAL "SHARED")
    set_target_properties(harness_consumer_c PROPERTIES
        BUILD_RPATH "${AERON_BUILD_DIR}/lib"
        INSTALL_RPATH "${AERON_BUILD_DIR}/lib")
endif()

add_executable(harness_price_server_c ../harness/price_server.c)
target_include_directories(harness_price_server_c PRIVATE "${AERON_INCLUDE_DIR}")
target_link_libraries(harness_price_server_c PRIVATE "${AERON_LIB}")

if(APPLE)
    target_link_libraries(harness_price_server_c PRIVATE "${CF_COREFOUNDATION}")
endif()

if(AERON_LIB_TYPE STREQUAL "SHARED")
    set_target_properties(harness_price_server_c PROPERTIES
        BUILD_RPATH "${AERON_BUILD_DIR}/lib"
        INSTALL_RPATH "${AERON_BUILD_DIR}/lib")
endif()
