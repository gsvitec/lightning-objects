# LMDB test

add_definitions(-DFlexisPersistence_EXPORTS)

set(DEP_SRC ../../kvstore.cpp ../../../kv/kvlibtraits.cpp)

add_executable(LmdbTest test.cpp testclasses.cpp ${DEP_SRC}
    $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:FlexisOverlays>
    $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:ModuleManualPerspectiveFinder>
    $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:ModuleCameraCalibration>
    $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:ModuleMedianFlowTracker>
)
add_dependencies(LmdbTest LmdbStore FlexisOverlays)

add_executable(LmdbBench bench.cpp testclasses.cpp ${DEP_SRC}
    $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:FlexisOverlays>
    $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:ModuleManualPerspectiveFinder>
    $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:ModuleCameraCalibration>
    $<TARGET_OBJECTS:LmdbStore> $<TARGET_OBJECTS:ModuleMedianFlowTracker>
)
add_dependencies(LmdbBench LmdbStore FlexisOverlays)

target_link_libraries(LmdbTest FlexisBase ${OpenCV_LIBS})
target_link_libraries(LmdbBench FlexisBase ${OpenCV_LIBS})
if(UNIX)
    target_link_libraries(LmdbTest pthread)
    target_link_libraries(LmdbBench pthread)
endif()
if(WIN32)
    target_link_libraries(LmdbTest ntdll)
    target_link_libraries(LmdbBench ntdll)
endif()

set(TEST_INCLUDES
        ${CMAKE_CURRENT_SOURCE_DIR}/../..
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${FLEXISLIB_DIR}/Overlays
        ${FLEXIS_LIB_DIR}/Modules/ManualPerspectiveFinder
        ${FLEXIS_LIB_DIR}/Modules/CameraCalibration
        ${FLEXIS_LIB_DIR}/Modules/MedianFlowTracker
        ${FLEXISLIB_DIR}/Player
        ${FLEXIS_LIB_DIR}/FlexisGST
        ${FLEXIS_LIB_DIR}/Util
        ${OpenCV_INCLUDE_DIRS}
        ${FLEXISLIB_DIR}/Persistence
        ${FLEXISLIB_DIR}/FlexisBase)

target_include_directories(LmdbTest PUBLIC ${TEST_INCLUDES})
target_include_directories(LmdbBench PUBLIC ${TEST_INCLUDES} ${CMAKE_CURRENT_SOURCE_DIR}/../liblmdb)

add_dependencies(LmdbTest LmdbLoad LmdbDump LmdbStat)
