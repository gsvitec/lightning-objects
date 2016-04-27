# LMDB test

add_executable(LmdbTest test.cpp testclasses.cpp test_classupdate.cpp $<TARGET_OBJECTS:FlexisKVStore> $<TARGET_OBJECTS:LmdbStore>)
add_dependencies(LmdbTest FlexisKVStore LmdbStore)

add_executable(LmdbBench bench.cpp testclasses.cpp $<TARGET_OBJECTS:FlexisKVStore> $<TARGET_OBJECTS:LmdbStore>)
add_dependencies(LmdbBench FlexisKVStore LmdbStore)

if(UNIX)
    target_link_libraries(LmdbTest pthread)
    target_link_libraries(LmdbBench pthread)
endif()
if(WIN32)
    target_link_libraries(LmdbTest ntdll)
    target_link_libraries(LmdbBench ntdll)
endif()

set(TEST_INCLUDES $<TARGET_PROPERTY:FlexisKVStore,INTERFACE_INCLUDE_DIRECTORIES>)

target_include_directories(LmdbTest PUBLIC ${TEST_INCLUDES})
target_include_directories(LmdbBench PUBLIC ${TEST_INCLUDES} ${CMAKE_CURRENT_SOURCE_DIR}/../liblmdb)

add_dependencies(LmdbTest LmdbLoad LmdbDump LmdbStat)
