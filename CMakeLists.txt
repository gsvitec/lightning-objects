set(SOURCE_FILES kvstore.cpp)
set(HEADER_FILES kvstore.h kvtraits.h kvbuf.h)
set(OBJECTS)

add_subdirectory(lmdb)

add_library(FlexisKVStore OBJECT ${SOURCE_FILES} ${OBJECTS})

target_include_directories(FlexisKVStore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
