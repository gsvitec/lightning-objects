# LMDB

set(LmdbStore_SOURCES lmdb_kvstore.cpp liblmdb/mdb.c liblmdb/midl.c)
set(lo_dump_SOURCES lmdb_kvdump.cpp ../kvstore.cpp lmdb_kvstore.cpp liblmdb/mdb.c liblmdb/midl.c)

add_definitions(-DFlexisPersistence_EXPORTS)

add_library(LmdbStore OBJECT ${LmdbStore_SOURCES})

add_executable(lo_dump ${lo_dump_SOURCES})

if(UNIX)
set(MDBLOAD_SOURCES liblmdb/mdb_load.c liblmdb/mdb.c liblmdb/midl.c)
set(MDBDUMP_SOURCES liblmdb/mdb_dump.c liblmdb/mdb.c liblmdb/midl.c)
set(MDBSTAT_SOURCES liblmdb/mdb_stat.c liblmdb/mdb.c liblmdb/midl.c)
else()
set(MDBLOAD_SOURCES liblmdb/mdb_load.c liblmdb/mdb.c liblmdb/midl.c liblmdb/getopt.c)
set(MDBDUMP_SOURCES liblmdb/mdb_dump.c liblmdb/mdb.c liblmdb/midl.c liblmdb/getopt.c)
set(MDBSTAT_SOURCES liblmdb/mdb_stat.c liblmdb/mdb.c liblmdb/midl.c liblmdb/getopt.c)
endif()

add_executable(LmdbLoad ${MDBLOAD_SOURCES})
target_include_directories(LmdbLoad PRIVATE liblmdb)
add_executable(LmdbDump ${MDBDUMP_SOURCES})
target_include_directories(LmdbDump PRIVATE liblmdb)
add_executable(LmdbStat ${MDBSTAT_SOURCES})
target_include_directories(LmdbStat PRIVATE liblmdb)

if(UNIX)
    target_link_libraries(LmdbLoad pthread)
    target_link_libraries(LmdbDump pthread)
    target_link_libraries(LmdbStat pthread)
    target_link_libraries(lo_dump pthread)
endif()
if(WIN32)
    target_link_libraries(LmdbLoad ntdll)
    target_link_libraries(LmdbDump ntdll)
    target_link_libraries(LmdbStat ntdll)
    target_link_libraries(lo_dump ntdll)
endif()

target_include_directories(LmdbStore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../..)
target_include_directories(LmdbStore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_include_directories(LmdbStore PRIVATE liblmdb)

target_include_directories(lo_dump PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../..)
target_include_directories(lo_dump PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_include_directories(lo_dump PRIVATE liblmdb)

add_subdirectory(test)
