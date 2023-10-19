cmake_minimum_required(VERSION 3.13)
project(kv-store)
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

#message(${CMAKE_VERSION})
add_definitions(-Wno-unknown-pragmas)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -Wall")

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/global.cmake)

include_directories(include)

add_subdirectory(kv-store-server)
add_subdirectory(kv-store-client)
#add_custom_target(ASSETS_COPY ALL VERBATIM COMMAND ${CMAKE_COMMAND} -E copy_directory "${CMAKE_SOURCE_DIR}/assets" "${CMAKE_BINARY_DIR}/assets")