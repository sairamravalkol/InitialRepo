//
// Created by norig on 6/25/15.
//

#ifndef TVG_IGOR_ADJACENTNVVERTEX_H
#define TVG_IGOR_ADJACENTNVVERTEX_H



#include "AdjacentNVStructure.h"
#include "../../Parser/Header/AdjacentData.h"

#include <string>

#include <stdio.h>
#include <iostream>



class AdjacentNVStructure;


class AdjacentNVVertex
{
private:
    //
    AdjacentData m_adjData;
    AdjacentNVStructure m_adjStructure;

public:
    //
    AdjacentNVVertex()
    {

    }
    ~AdjacentNVVertex()
    { };

    AdjacentNVVertex(const AdjacentNVVertex& elem);
    const AdjacentNVVertex & operator=(const AdjacentNVVertex& elem);


    void SetData(const tvg_long_counter& lUID, tvg_time tEpoch, tvg_index lIndex, tvg_vertex_id lNewNodeID, bool bSource, SameNodeNVLink l = tvg_index(-1), MODIFICATION_TYPE modificationType = MODIFICATION_TYPE::ADDITION);




    tvg_index GetIndex()
    {
        return m_adjData.GetIndexInData();
    }


    tvg_vertex_id GetID()
    {
        return m_adjData.GetID();
    }

    tvg_time GetEpoch()
    {
        return m_adjData.GetEpoch();
    }


    const SameNodeNVLink& GetSameNodePrev()
    {
        return m_adjStructure.GetSameNodePrev();
    }


    bool IsSource()
    {
        return m_adjData.IsSource();
    }

    void Submit(std::ofstream& ofstream);


//    static std::pair<int,int> AddToPair(const std::pair<int, int> &p, int amount) {
//        return std::make_pair(p.first+amount,p.second);
//    }
//    static int GetFromPair(const std::pair<int, int> &p){
//        return p.first;
//    }
};


/*


 // Old version
 //
class AdjacentNVVertex
{
private:
    //
    AdjacentData m_adjData;
    AdjacentNVStructure m_adjStructure;

public:
    //
    AdjacentNVVertex()
    {

    }
    ~AdjacentNVVertex()
    { };

    AdjacentNVVertex(const AdjacentNVVertex& elem);
    const AdjacentNVVertex & operator=(const AdjacentNVVertex& elem);


    void SetData(const tvg_long_counter& lUID, tvg_time tEpoch, tvg_index lIndex, tvg_vertex_id lNewNodeID, const std::unique_ptr<SameNodeNVLink>& pCurrNodeLink, const std::unique_ptr<SameNodeNVLink>& pOtherNodeLink, bool bSource, MODIFICATION_TYPE modificationType = MODIFICATION_TYPE::ADDITION);
    void SetAdjData(const tvg_long_counter& lUID, tvg_time tEpoch, tvg_index lIndex, tvg_vertex_id lNewNodeID, bool bSource, MODIFICATION_TYPE modificationType = MODIFICATION_TYPE::ADDITION);
    void SetStructureData(const tvg_vertex_id& lThisVertexID, const tvg_index& lThisVertexIndex,
                          const tvg_vertex_id& lSecondVertexID, const tvg_index& lSecondVertexIndex);

    tvg_index GetIndex()
    {
        return m_adjData.GetIndexInData();
    }


    tvg_vertex_id GetID()
    {
        return m_adjData.GetID();
    }

    tvg_time GetEpoch()
    {
        return m_adjData.GetEpoch();
    }


    bool IsSource()
    {
        return m_adjData.GetIsSource();
    }

    void Submit(std::ofstream& ofstream);


//    static std::pair<int,int> AddToPair(const std::pair<int, int> &p, int amount) {
//        return std::make_pair(p.first+amount,p.second);
//    }
//    static int GetFromPair(const std::pair<int, int> &p){
//        return p.first;
//    }
};
 */

#endif //TVG_IGOR_ADJACENTNVVERTEX_H
