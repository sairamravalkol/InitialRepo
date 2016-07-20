//
// Created by norig on 6/25/15.
//



#include "../Header/AdjacentNVVertex.h"
#include <fstream>

AdjacentNVVertex::AdjacentNVVertex(const AdjacentNVVertex& elem)
{
    *this = elem;
}

const AdjacentNVVertex &AdjacentNVVertex::operator=(const AdjacentNVVertex& elem)
{
    m_adjData=elem.m_adjData;
    m_adjStructure = elem.m_adjStructure;
    return *this;
}



void AdjacentNVVertex::SetData(const tvg_long_counter& lUID, tvg_time tEpoch, tvg_index lIndex, tvg_vertex_id lNewNodeID, bool bSource, SameNodeNVLink l, MODIFICATION_TYPE modificationType)
{
    m_adjData.SetData(lUID, tEpoch, lNewNodeID, lIndex, bSource, modificationType);
    m_adjStructure.SetData(l);
}




void AdjacentNVVertex::Submit(std::ofstream& ofstream)
{
    ofstream<<"---------------------------"<<std::endl;
    ofstream<<"Index: "<<m_adjData.GetIndexInData()<<std::endl;
    ofstream<<"ID: "<<m_adjData.GetID()<<std::endl;
    ofstream<<"Epoch: "<<m_adjData.GetEpoch()<<std::endl;
    ofstream<<"Source: "<<m_adjData.IsSource()<<std::endl;
    ofstream<<"Prev: "<<m_adjStructure.GetSameNodePrev().GetIndex().GetString()<<std::endl;

    ofstream<<std::endl;

    //   ofstream<<"Chain Link: Vertex "<<m_adjStructure->GetLinkNVChainStructure()->GetVertexID()<<", Index "<<m_adjStructure.GetLinkNVChainStructure()->GetIndex()<<std::endl;
}


/*
AdjacentNVVertex::AdjacentNVVertex(const AdjacentNVVertex& elem)
{
    *this = elem;
}

const AdjacentNVVertex &AdjacentNVVertex::operator=(const AdjacentNVVertex& elem)
{
    m_adjData=elem.m_adjData;
    m_adjStructure= elem.m_adjStructure;
    return *this;
}



void AdjacentNVVertex::SetData(const tvg_long_counter& lUID, tvg_time tEpoch, tvg_index lIndex, tvg_vertex_id lNewNodeID, const std::unique_ptr<SameNodeNVLink>& pCurrNodeLink, const std::unique_ptr<SameNodeNVLink>& pOtherNodeLink, bool bSource, MODIFICATION_TYPE modificationType)
{
    m_adjData.SetData(lUID, tEpoch, lNewNodeID, lIndex, bSource, modificationType);
    m_adjStructure.SetData(pCurrNodeLink->GetIndex(), pOtherNodeLink->GetVertexID(), pOtherNodeLink->GetIndex());
}



void AdjacentNVVertex::SetAdjData(const tvg_long_counter& lUID, tvg_time tEpoch, tvg_index lIndex, tvg_vertex_id lNewNodeID, bool bSource, MODIFICATION_TYPE modificationType)
{
    m_adjData.SetData(lUID, tEpoch, lNewNodeID, lIndex, bSource, modificationType);
}


void AdjacentNVVertex::SetStructureData(const tvg_vertex_id& lThisVertexID, const tvg_index& lThisVertexIndex,
                                        const tvg_vertex_id& lSecondVertexID, const tvg_index& lSecondVertexIndex)
{
    m_adjStructure.SetData(lThisVertexID, lThisVertexIndex, lSecondVertexID, lSecondVertexIndex);
}


void AdjacentNVVertex::Submit(std::ofstream& ofstream)
{
    ofstream<<"---------------------------"<<std::endl;
    ofstream<<"Index: "<<m_adjData.GetIndexInData()<<std::endl;
    ofstream<<"ID: "<<m_adjData.GetID()<<std::endl;
    ofstream<<"Epoch: "<<m_adjData.GetEpoch()<<std::endl;
    ofstream<<"Source: "<<m_adjData.GetIsSource()<<std::endl;

    ofstream<<std::endl;

    if (m_adjStructure.GetEdgeReplicationVertexID().IsValid())
        ofstream<<"Neighbor Link: Vertex "<<m_adjStructure.GetEdgeReplicationVertexID()<<", Index "<<m_adjStructure.GetEdgeReplicationIndex()<<std::endl;

 //   ofstream<<"Chain Link: Vertex "<<m_adjStructure->GetLinkNVChainStructure()->GetVertexID()<<", Index "<<m_adjStructure.GetLinkNVChainStructure()->GetIndex()<<std::endl;
}
*/