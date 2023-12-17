project(kv-store-server VERSION 1.0.0)
configure_file(global-config.h.in global-config.h @ONLY)

executable()
target_link_libraries(kv-store-server pthread dl lzf.a)
target_link_options(kv-store-server PRIVATE -rdynamic)
target_include_directories(kv-store-server PRIVATE . ${PROJECT_BINARY_DIR})

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