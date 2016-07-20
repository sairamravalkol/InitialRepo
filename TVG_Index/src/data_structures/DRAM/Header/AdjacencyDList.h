//
// Created by norig on 6/29/15.
//

#ifndef TVG_IGOR_ADJACENCYDLIST_H
#define TVG_IGOR_ADJACENCYDLIST_H


#include <iostream>
#include "AdjacentDStructure.h"
//#include "../../Graph/Header/MyVertex.h"
#include "../../NVRAM/Header/AdjacentNVVertex.h"
#include "../../../../include/Globals.h"
#include <log4cpp/Category.hh>


class AdjacencyDList
{
protected:
    //
    tvg_vertex_id m_lVertexID;

    log4cpp::Category& _log_root;
//    Vertica::ServerInterface& _log_root;
    std::map<tvg_index, std::unique_ptr<AdjacentDStructure>> m_mapIndexToDSStructure; // vertex index to the data
    std::map<tvg_vertex_id, tvg_index> m_mapSameValueToLastLocation;


    static tvg_long_counter m_lSizeIndexToDSStructure;
    static tvg_long_counter m_lSizeSameValueToLastLocation;


    const tvg_short_counter m_lIndexSizeof;
    const tvg_short_counter m_lVertexSizeof;


public:
    //
    AdjacencyDList(tvg_vertex_id lVertexID);
    ~AdjacencyDList();


    std::unique_ptr<AdjacentDStructure> GetNextIncomplete(tvg_index& index);

    std::unique_ptr<AdjacentDStructure> UpdateConnection(tvg_time tCreationEpoch, tvg_index lIndex, std::unique_ptr<AdjacentNVVertex> pLastAdded, tvg_vertex_id lNewNodeID, tvg_index& indexOfComplete);


    tvg_long_counter GetRealSameValueToLastLocationSize();
    tvg_long_counter GetRealIndexToDSStructureSize();

};


#endif //TVG_IGOR_ADJACENCYDLIST_H


