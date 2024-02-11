project(kv-store-server VERSION 1.0.0)
#option(BUILD_SUPPORTED_REDIS_VERSION "The compiled command follows the version support of Redis" 7)
set(BUILD_SUPPORTED_REDIS_VERSION "7.0.0" CACHE STRING "The compiled command follows the version support of Redis")

string(REPLACE "." ";" VERSION_LIST ${BUILD_SUPPORTED_REDIS_VERSION})
list(GET VERSION_LIST 0 BUILD_SUPPORTED_REDIS_VERSION_MAJOR)
list(GET VERSION_LIST 1 BUILD_SUPPORTED_REDIS_VERSION_MINOR)
list(GET VERSION_LIST 2 BUILD_SUPPORTED_REDIS_VERSION_PATCH)

configure_file(global-config.h.in global-config.h @ONLY)

executable()
target_link_libraries(kv-store-server pthread dl lzf.a)
target_link_options(kv-store-server PRIVATE -rdynamic)
target_include_directories(kv-store-server PRIVATE . ${PROJECT_BINARY_DIR})

add_dependencies(${PROJECT_NAME} lzf_static)
#add_custom_command(TARGET ${PROJECT_NAME} PRE_BUILD
#                   COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target lzf_static -- -j
#                   WORKING_DIRECTORY ${LIBRARY_OUTPUT_PATH}
#                   COMMENT "build lzf ...")

add_custom_command(TARGET kv-store-server
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/kv-store.conf ${EXECUTABLE_OUTPUT_PATH}
                   WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                   COMMENT "copy kv-store.conf ...")

add_custom_target(COPY_CONFIG
                  ALL
                  VERBATIM
                  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/kv-store.conf ${EXECUTABLE_OUTPUT_PATH}
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                  COMMENT "copy kv-store.conf ...")