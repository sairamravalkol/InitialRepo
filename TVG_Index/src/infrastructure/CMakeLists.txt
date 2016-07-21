cmake_minimum_required(VERSION 3.3)
project(infrastructure)

set(CMAKE_BUILD_TYPE Debug)

add_library(infrastructure BufferManager.cpp BufferManager.h TargetNVM.h VertexProxy.cpp VertexProxy.h)

target_include_directories(infrastructure PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})