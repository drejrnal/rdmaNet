cmake_minimum_required(VERSION 3.10)
project(rdmaComm)

set(CMAKE_CXX_STANDARD 14)

file( GLOB netSrcs ${PROJECT_SOURCE_DIR}/*.cpp )
message( "${netSrcs}")
add_library( nettool STATIC ${netSrcs} )
target_link_libraries( nettool pthread ibverbs)


file( GLOB TEST_SRCS ${PROJECT_SOURCE_DIR}/test/*.cpp )

foreach( TEST_SRC ${TEST_SRCS} )
    message( "test file: ${TEST_SRC}" )
    get_filename_component( TARGET_NAME ${TEST_SRC} NAME_WE )
    message( "test targets name: ${TARGET_NAME} " )
    add_executable( ${TARGET_NAME} ${TEST_SRC} )
    target_link_libraries( ${TARGET_NAME} nettool )
endforeach()