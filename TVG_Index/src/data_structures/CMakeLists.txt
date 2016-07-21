cmake_minimum_required(VERSION 3.2)
project(data_structures)

set(LOG4CPP_INCLUDE_PATH /usr/local/include/log4cpp)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++14")
#set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${data_structures_SOURCE_DIR}/../../lib)


add_library(data_structures Graph/Source/MyGraph.cpp
        Parser/Header/Modification.h
        Parser/Header/DBSecurityScheme.h
        Parser/Source/AdjacentData.cpp
        Parser/Header/AdjacentData.h
        DRAM/Source/AdjacentDStructure.cpp
        DRAM/Header/AdjacentDStructure.h
        DRAM/Source/AdjacencyDList.cpp
        DRAM/Header/AdjacencyDList.h
        DRAM/Source/AdjacencyDList.cpp
        DRAM/Header/AdjacencyDList.h
        NVRAM/Source/NVRepository.cpp
        NVRAM/Header/NVRepository.h
        NVRAM/Header/NVChainStructure.h
        NVRAM/Source/AdjacentNVVertex.cpp
        NVRAM/Header/AdjacentNVVertex.h
        NVRAM/Source/ChainAdditionalData.cpp
        NVRAM/Header/ChainAdditionalData.h
        NVRAM/Source/NVChainStructure.cpp
        NVRAM/Source/AdjacentNVStructure.cpp
        NVRAM/Header/AdjacentNVStructure.h
#        ../infrastructure/BufferManager.cpp
#        ../infrastructure/BufferManager.h
#        ../infrastructure/TargetNVM.h
        General/Source/MyHistogram.cpp
        General/Header/MyVector.hpp
        General/Header/MyHistogram.h
        General/Source/NeighborMetaData.cpp
        General/Header/NeighborMetaData.h
        General/Header/tvg_vertex_id.h
        General/Header/tvg_index.h
        General/Header/tvg_time.h
        General/Header/Link.h
        General/Source/Link.cpp
        General/Header/Stats.h
        Graph/Header/GraphMemoryManager.h
        Graph/Source/MyEdge.cpp
        Graph/Header/MyGraph.h
        Graph/Source/MyVertex.cpp
        Graph/Header/MyVertex.h
        Graph/Header/MyEdge.h
        Graph/Header/DebugInfo.h
        Graph/Source/DebugInfo.cpp
        Graph/Source/MyVertexSelect.cpp
        Graph/Header/MyVertexSelect.h
        Graph/Header/MyGraphSelect.h
        Graph/Source/MyGraphSelect.cpp
        Graph/Source/PerformanceMeter.cpp
        Graph/Header/PerformanceMeter.h)

target_include_directories(data_structures PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
#add_dependencies(data_structures infrastructure_utils)