cmake_minimum_required(VERSION 3.3)
project(TVG_Index)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")

#set(CMAKE_BUILD_TYPE Release)
set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${TVG_Index_SOURCE_DIR}/bin)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${TVG_Index_SOURCE_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${TVG_Index_SOURCE_DIR}/bin)

set(vertica_sdk /opt/vertica/sdk/include)
include_directories(${vertica_sdk})

add_subdirectory(src/data_structures)
add_subdirectory(src/infrastructure)

find_package(Boost 1.58.0  COMPONENTS  program_options  filesystem system)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
endif()

find_library(LOG4CPP liblog4cpp.so)
include_directories(${LOG4CPP_INCLUDE_PATH})

set(SOURCE_FILES main.cpp)
set(UDF_SOURCE_FILES ${vertica_sdk}/Vertica.cpp
        src/vertica_udf/ProcessInput.cpp
        src/vertica_udf/ProcessInput.h
        src/vertica_udf/QueryDS.cpp
        src/vertica_udf/QueryDS.h
        )
add_library(TVG_Index_udf SHARED ${UDF_SOURCE_FILES})

# copy the udf to tmp where dbeaver can access and load into vertica
add_custom_command(TARGET TVG_Index_udf POST_BUILD COMMAND ${CMAKE_COMMAND} -E copy_directory ${TVG_Index_SOURCE_DIR}/bin /tmp/)
target_link_libraries(TVG_Index_udf
        data_structures
        infrastructure
        ${Boost_LIBRARIES}
        ${LOG4CPP})
add_dependencies(TVG_Index_udf data_structures )
add_dependencies(TVG_Index_udf infrastructure )

