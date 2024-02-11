add_definitions(-DKV_STORE_CLIENT)

executable()

target_include_directories(kv-store-client PRIVATE ../kv-store-server)