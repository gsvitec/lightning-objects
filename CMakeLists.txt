set(SOURCE_FILES kvstore.cpp)
set(HEADER_FILES kvstore.h kvtraits.h kvbuf.h)
set(OBJECTS)

add_subdirectory(lmdb)

add_library(FlexisKVStore OBJECT ${SOURCE_FILES} ${OBJECTS})

target_include_directories(FlexisKVStore PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

find_package(Doxygen)
if(DOXYGEN_FOUND)
    set(LO_APIDOC_IN ${CMAKE_CURRENT_SOURCE_DIR})
    set(LO_APIDOC_OUT ${CMAKE_CURRENT_SOURCE_DIR}/doc/api)
    set(LO_APIDOC_EXCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/lmdb/liblmdb")
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/doc/Doxyfile.in ${CMAKE_CURRENT_SOURCE_DIR}/doc/api/Doxyfile @ONLY)
    add_custom_target(LO_APIDOC
            ${DOXYGEN_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/doc/api/Doxyfile
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/doc/api
            COMMENT "Generating LightningObjects API documentation" VERBATIM
            )
endif(DOXYGEN_FOUND)
