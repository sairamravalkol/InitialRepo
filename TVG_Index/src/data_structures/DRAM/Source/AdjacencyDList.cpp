//
// Created by norig on 6/29/15.
//


#include <log4cpp/Category.hh>
#include "../Header/AdjacencyDList.h"


tvg_long_counter AdjacencyDList::m_lSizeIndexToDSStructure = 0;
tvg_long_counter AdjacencyDList::m_lSizeSameValueToLastLocation = 0;


AdjacencyDList::AdjacencyDList(tvg_vertex_id lVertexID) : _log_root(log4cpp::Category::getRoot()), m_lVertexID(lVertexID), m_lIndexSizeof(sizeof(tvg_index)), m_lVertexSizeof(sizeof(tvg_vertex_id))
{

/*
    // Creat first demi index -1
    //
    std::unique_ptr<AdjacentDStructure> pNewDSStructure = std::make_unique<AdjacentDStructure>(-1, -1);
    pNewDSStructure->UpdateConnection(-1, -1, -1);

    m_mapIndexToDSStructure.insert(std::make_pair(-1, std::move(pNewDSStructure)));
*/
}


AdjacencyDList::~AdjacencyDList()
{

    std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>::iterator iter;
    iter = m_mapIndexToDSStructure.begin();
    for ( ; iter != m_mapIndexToDSStructure.end() ; iter++)
    {
        iter->second.reset();
    }

    m_mapIndexToDSStructure.clear();

}

std::unique_ptr<AdjacentDStructure> AdjacencyDList::GetNextIncomplete(tvg_index& index)
{
    std::unique_ptr<AdjacentDStructure> pCompleteRes;

    std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>::iterator iter = m_mapIndexToDSStructure.begin();
    while (iter != m_mapIndexToDSStructure.end())
    {
        if (iter->second == NULL)
            m_mapIndexToDSStructure.erase(iter);
        else
        {
            index = iter->first;
            pCompleteRes = std::move(iter->second);
            m_mapIndexToDSStructure.erase(iter);
            break;
        }
    }

    return std::move(pCompleteRes);
}



std::unique_ptr<AdjacentDStructure> AdjacencyDList::UpdateConnection(tvg_time tCreationEpoch, tvg_index lIndex, std::unique_ptr<AdjacentNVVertex> pLastAdded, tvg_vertex_id lNewNodeID, tvg_index& indexOfComplete)
{
    std::unique_ptr<AdjacentDStructure> pCompleteRes;

    // First time the index is added, should always be empty
    //
    std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>::iterator iter = m_mapIndexToDSStructure.find(lIndex);
    if (iter != m_mapIndexToDSStructure.end())
    {
//        log4cpp::Category::getRoot().error("AdjacencyDList::UpdateConnection 1: the new index " + std::to_string(lIndex) + " already exists");
        return pCompleteRes;
    }


    std::unique_ptr<AdjacentDStructure> pNewDSStructure = std::make_unique<AdjacentDStructure>(lIndex, tCreationEpoch);
    if (pLastAdded)
        pNewDSStructure->UpdateConnection(tCreationEpoch, pLastAdded->GetIndex(), pLastAdded->GetID());
    else
        pNewDSStructure->UpdateConnection(tCreationEpoch, -1, -1);





    // Update others
    //
    if (!m_mapSameValueToLastLocation.empty())
    {
        std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>::iterator iterLastAddedDSStructure = m_mapIndexToDSStructure.find(lIndex - tvg_index(1));
        if (iterLastAddedDSStructure == m_mapIndexToDSStructure.end()) // should never happen
        {
//            log4cpp::Category::getRoot().error("AdjacencyDList::UpdateConnection 3: Could not find the structure in m_mapIndexToDSStructure for the last added location" + std::to_string((lIndex - tvg_index(1))));
            //               return NULL;
        }


        else
        {
            std::map<tvg_vertex_id, tvg_index>::iterator iterSameValue = m_mapSameValueToLastLocation.find(lNewNodeID);
            if (iterSameValue == m_mapSameValueToLastLocation.end()) // if no same value
            {
                pNewDSStructure->UpdateLinks(m_lVertexID, iterLastAddedDSStructure->second);
            }
            else
            {
                std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>::iterator iterSameLocation = m_mapIndexToDSStructure.find(iterSameValue->second);

                if (iterSameLocation == m_mapIndexToDSStructure.end())
                {
//                    log4cpp::Category::getRoot().error("AdjacencyDList::UpdateConnection 2: the same value element " + std::to_string(iterSameValue->second) + " exists, but could not be found in m_mapIndexToDSStructure");
                    //           return pCompleteRes;
                }
                else
                {
                    pNewDSStructure->UpdateLinks(m_mapIndexToDSStructure, lIndex, m_lVertexID, iterSameLocation->second, iterSameLocation->first, iterLastAddedDSStructure->second);
                    pCompleteRes = std::move(iterSameLocation->second);
                    indexOfComplete = iterSameLocation->first;
                    m_mapIndexToDSStructure.erase(iterSameLocation);

                }
            }
        }
    }
    else
    {
        int iDeb = 6;
//        std::cout<<"Empty m_mapSameValueToLastLocation"<<std::endl;
    }


 //   m_lSizeIndexToDSStructure -= m_mapIndexToDSStructure.size();
    m_mapIndexToDSStructure[lIndex] = std::move(pNewDSStructure);
//    m_lSizeIndexToDSStructure += m_mapIndexToDSStructure.size();
 //   if ((m_lSizeIndexToDSStructure % 1000) == 0)
//        std::cout<<": m_lSizeIndexToDSStructure = "<<m_lSizeIndexToDSStructure<<std::endl;



    // overwrite the last instance of the ID
   // //
 //   m_lSizeSameValueToLastLocation -= m_mapSameValueToLastLocation.size();
    m_mapSameValueToLastLocation[lNewNodeID] = lIndex;
 //   m_lSizeSameValueToLastLocation += m_mapSameValueToLastLocation.size();
 //   if ((m_lSizeSameValueToLastLocation % 1000) == 0)
 //       std::cout<<": m_lSizeSameValueToLastLocation = "<<m_lSizeSameValueToLastLocation<<std::endl;



    if (m_mapIndexToDSStructure.size() != m_mapSameValueToLastLocation.size())
        std::cout<<": Error: "<<m_mapIndexToDSStructure.size()<<", "<<m_mapSameValueToLastLocation.size()<<std::endl;


    return std::move(pCompleteRes);
}



tvg_long_counter AdjacencyDList::GetRealSameValueToLastLocationSize()
{
    return (m_mapSameValueToLastLocation.size() * (m_lIndexSizeof + m_lVertexSizeof));
}


tvg_long_counter AdjacencyDList::GetRealIndexToDSStructureSize()
{
    tvg_long_counter lRes = 0;
    std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>::iterator iter = m_mapIndexToDSStructure.begin();
    for ( ; iter != m_mapIndexToDSStructure.end() ; iter++)
    {
        lRes += m_lIndexSizeof;
        lRes += iter->second->GetRealIndexToDSStructureSize();
    }


    return lRes;
}