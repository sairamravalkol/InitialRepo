cmake_minimum_required(VERSION 3.3)
project(TVG_Index)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

#set(CMAKE_BUILD_TYPE Release)
set(CMAKE_BUILD_TYPE Debug)


set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TVG_Index_SOURCE_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${TVG_Index_SOURCE_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${TVG_Index_SOURCE_DIR}/bin)


include_directories( ./lib/vertica/sdk/include)

set(SOURCE_FILES main.cpp )
set(UDF_SOURCE_FILES ./lib/vertica/sdk/include/Vertica.cpp src/vertica_udf/Add2Ints.cpp)

add_library(TVG_Index_udf SHARED  ${UDF_SOURCE_FILES})
#add_executable(TVG_Index ${SOURCE_FILES})

