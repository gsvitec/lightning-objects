# LMDB test

set(SOURCE_FILES main.cpp ../../kvstore.cpp)

add_definitions(-DFlexisPersistence_EXPORTS)

add_executable(LmdbTest ${SOURCE_FILES} $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:FlexisOverlays>)
add_dependencies(LmdbTest LmdbStore FlexisOverlays)

add_executable(LmdbBench bench.cpp ../../kvstore.cpp $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:FlexisOverlays>)
add_dependencies(LmdbBench LmdbStore FlexisOverlays)

target_link_libraries(LmdbTest FlexisBase)
target_link_libraries(LmdbBench FlexisBase)
if(UNIX)
    target_link_libraries(LmdbTest pthread)
    target_link_libraries(LmdbBench pthread)
endif()

set(TEST_INCLUDES
        ${CMAKE_CURRENT_SOURCE_DIR}/../..
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${FLEXISLIB_DIR}/Overlays
        ${FLEXISLIB_DIR}/Player
        ${FLEXISLIB_DIR}/Persistence
        ${FLEXISLIB_DIR}/FlexisBase)

target_include_directories(LmdbTest PUBLIC ${TEST_INCLUDES})
target_include_directories(LmdbBench PUBLIC ${TEST_INCLUDES} ${CMAKE_CURRENT_SOURCE_DIR}/../liblmdb)

add_dependencies(LmdbTest LmdbLoad LmdbDump LmdbStat)