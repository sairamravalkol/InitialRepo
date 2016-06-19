cmake_minimum_required(VERSION 3.3)
project(TVG_Index)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
include_directories( ./lib/sdk/include)
set(SOURCE_FILES main.cpp ./lib/sdk/include/Vertica.cpp udf/helloworld.cpp udf/helloworld.h)
add_executable(TVG_Index ${SOURCE_FILES})

#set(VERTICA_SDK ./lib/sdk/include/Vertica.cpp)
#/home/hayune/TVG_Vertica/TVG_Index/lib/sdk/include/V
# target_link_libraries(${VERTICA_SDK})