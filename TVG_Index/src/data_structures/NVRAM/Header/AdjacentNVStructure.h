//
// Created by norig on 6/24/15.
//

#ifndef TVG_IGOR_ADJACENTNVSTRUCTURE_H
#define TVG_IGOR_ADJACENTNVSTRUCTURE_H

#include <string>

#include <stdio.h>


#include "../../General/Header/Link.h"
//#include "NVChainStructure.h"
//#include "../../Graph/Header/MyVertex.h"
#include <memory>

// Here we keep the data that can immediately be initialized and coppied to the NVRam
//
/*
class AdjacentNVStructure
{
private:
    OtherNodeNVLink m_linkEdgeReplication; // link to the neighbour adjacency list, to the same edge
    NVTemporalLink m_linkNVChainStructure;


//    NVChainStructure* m_pNVChainStructure;

public:
    //
    AdjacentNVStructure()
    {
        //   m_pNVChainStructure = new NVChainStructure();
    }
    ~AdjacentNVStructure()
    {
        //   if (m_pNVChainStructure)
        //       delete m_pNVChainStructure;

    }


    AdjacentNVStructure(const AdjacentNVStructure& elem)
    {
        // m_pNVChainStructure = new NVChainStructure(*(elem.m_pNVChainStructure));
        //   *this = elem;
    }
    const AdjacentNVStructure &operator=(const AdjacentNVStructure & elem)
    {
        m_linkEdgeReplication = elem.m_linkEdgeReplication;
        m_linkNVChainStructure = elem.m_linkNVChainStructure;
//    m_pNVChainStructure = new NVChainStructure(*(elem.m_pNVChainStructure));
        return *this;
    }


    void SetData(const tvg_index& lThisVertexIndex,
                 const tvg_vertex_id& lSecondVertexID, const tvg_index& lSecondVertexIndex)
    {
        m_linkEdgeReplication.SetVertexID(lSecondVertexID);
        m_linkEdgeReplication.SetIndex(lSecondVertexIndex);

        m_linkNVChainStructure.SetIndex(lThisVertexIndex);

    }



    tvg_index GetEdgeReplicationIndex()
    {
        return m_linkEdgeReplication.GetIndex();
    }

    tvg_vertex_id GetEdgeReplicationVertexID()
    {
        return m_linkEdgeReplication.GetVertexID();
    }


};
*/


class AdjacentNVStructure
{
private:
  //
    SameNodeNVLink m_lSameNodePrev;



public:
    //
    AdjacentNVStructure()
    {

    }
    ~AdjacentNVStructure()
    {

    }



    const AdjacentNVStructure &operator=(const AdjacentNVStructure & elem)
    {
        m_lSameNodePrev = elem.m_lSameNodePrev;
        return *this;
    }


     void SetData(const SameNodeNVLink& lSameNodePrev)
    {
        m_lSameNodePrev = lSameNodePrev;
    }

    void SetSameNodePrev(const SameNodeNVLink& lSameNodePrev)
    {
        m_lSameNodePrev = lSameNodePrev;
    }



    const SameNodeNVLink& GetSameNodePrev()
    {
        return m_lSameNodePrev;
    }

};


#endif //TVG_IGOR_ADJACENTNVSTRUCTURE_H
