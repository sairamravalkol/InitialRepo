//
// Created by norig on 6/24/15.
//

#ifndef TVG_IGOR_ADJACENTDSTRUCTURE_H
#define TVG_IGOR_ADJACENTDSTRUCTURE_H

#include <string>
#include <map>
#include <list>
#include <ctime>
#include <stdio.h>
#include <Vertica.h>
//#include <log4cpp/Category.hh>
#include "../../General/Header/Link.h"
#include "../../Parser/Header/AdjacentData.h"
//#include "../../../../include/Globals.h"


class AdjacentDStructure
{
protected:
    //
    tvg_index m_lIndex;
//    log4cpp::Category&_log_root;
    Vertica::ServerInterface& _log_root;
    tvg_time m_lConnectionEpoch;


    std::list<std::unique_ptr<SameNodeNVTemporalLink>> m_vChain; //

    SameNodeNVLink m_linkPrevEqualBefore; // previous location with the same content Left
    SameNodeNVLink m_linkPrevEqualAfter; // previous location with the same content Right

    SameNodeNVLink m_linkLastPointToMe; // link to the location that currently points to me in the chain




public:


    AdjacentDStructure(tvg_index lIndex, tvg_time lConnectionEpoch);

    ~AdjacentDStructure();

    std::list<std::unique_ptr<SameNodeNVTemporalLink>>& GetChain()
    {
        return m_vChain;
    }


    const tvg_index& GetIndex() const
    {
        return m_lIndex;
    }


    void UpdateConnection(tvg_time tCreationEpoch, tvg_index lIndex, tvg_vertex_id lID);
    void UpdateLinks(std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>& mapIndexToDSStructure, tvg_index lNewValueIndex,
                          tvg_vertex_id lThisNodeID, std::unique_ptr<AdjacentDStructure>& pPrevSameValue, tvg_index lPrevSameValueIndex,
                          std::unique_ptr<AdjacentDStructure>& pLastAddedDSStructure);


    void UpdateLinks(tvg_vertex_id lThisNodeID, std::unique_ptr<AdjacentDStructure>& pLastAddedDSStructure);

 /*   tvg_vertex_id GetVertexID()
    {
        return m_linkLastPointToMe.GetVertexID();
    }
*/
    tvg_long_counter GetRealIndexToDSStructureSize();
};


#endif //TVG_IGOR_ADJACENTDSTRUCTURE_H
