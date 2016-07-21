//
// Created by norig on 6/24/15.
//


#include <iostream>
//#include <log4cpp/Category.hh>
#include "../Header/AdjacentDStructure.h"





void AdjacentDStructure::UpdateConnection(tvg_time tCreationEpoch, tvg_index lIndex, tvg_vertex_id lID)
{
    // The link
    //
    std::unique_ptr<SameNodeNVTemporalLink> pNVTemporalLink = std::make_unique<SameNodeNVTemporalLink>(lIndex, tCreationEpoch);


    // Add the chain pointer from the new element to the last one added.
    // In case their are of the similar value, it will be updated later, as in other cases
    //
    m_vChain.push_back(std::move(pNVTemporalLink));


    // Currently no one point to me at the chain (the index will be -1)
    //
    m_linkLastPointToMe.SetData(lIndex);
}



void AdjacentDStructure::UpdateLinks(tvg_vertex_id lThisNodeID, std::unique_ptr<AdjacentDStructure>& pLastAddedDSStructure)
{
    // Set data to the prev same value
    //
    m_linkPrevEqualBefore.SetData(-1);


    // Update the last added - it now points to the new added element (it is currently not the last added!)
    //
    pLastAddedDSStructure->m_linkLastPointToMe.SetData(m_lIndex);

}

void AdjacentDStructure::UpdateLinks(std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>& mapIndexToDSStructure, tvg_index lNewValueIndex,
                                          tvg_vertex_id lThisNodeID, std::unique_ptr<AdjacentDStructure>& pPrevSameValue, tvg_index lPrevSameValueIndex,
                                          std::unique_ptr<AdjacentDStructure>& pLastAddedDSStructure)
{

        // Set data to the prev same value
        //
        m_linkPrevEqualBefore.SetData(lPrevSameValueIndex);
        pPrevSameValue->m_linkPrevEqualAfter.SetData(lNewValueIndex); // value, index


        // Update the last added - it now points to the new added element (it is currently not the last added!)
        //
        pLastAddedDSStructure->m_linkLastPointToMe.SetData(m_lIndex);


        // Update chain link (remove the equal from the chain)
        //

        // Update the chain (need to add only one pointer) for THIS vertex (only in case same value already occurred)
        //

        // 1. pPrevSameValue is the last same value appearance as of THIS
        //


        // 2. Find vertex u (iterPointsToPrevSame) that points to it in the chain
        //
        const tvg_index& lIndexPointsToSameValue = pPrevSameValue->m_linkLastPointToMe.GetIndex();
        std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>::iterator iterPointsToPrevSame = mapIndexToDSStructure.find(lIndexPointsToSameValue);
        if (iterPointsToPrevSame == mapIndexToDSStructure.end() && (m_lIndex != lIndexPointsToSameValue))
        {
//            log4cpp::Category::getRoot().error("AdjacentDStructure::UpdateOtherNodes 1: the prev same value element " + std::to_string(lIndexPointsToSameValue)  + " that points to me exists, but could not be found in pMapIndexToDSStructure");
        }
        else
        {
            // 3. Add to the u vertex chain a new link to the node
            //
            const std::unique_ptr<SameNodeNVTemporalLink>& pPrevLink = pPrevSameValue->m_vChain.back();
            std::unique_ptr<SameNodeNVTemporalLink> pNewLink = std::make_unique<SameNodeNVTemporalLink>(pPrevLink->GetIndex(),
                                                                                        m_lConnectionEpoch);

            if (m_lIndex == lIndexPointsToSameValue)
            {
                // m_vChain.back()->SetEpoch(m_lConnectionEpoch);

                if (!m_vChain.empty())
                {
                    std::list<std::unique_ptr<SameNodeNVTemporalLink>>::iterator iterLastChainValue = m_vChain.end();
                    iterLastChainValue--;
                    m_vChain.erase(iterLastChainValue);
                }
                else // should never happen ,a link should be already inserted earlier
                {
//                    log4cpp::Category::getRoot().error("AdjacentDStructure::UpdateOtherNodes 1.5: The chain is empty of vertex" + std::to_string(m_lIndex));
                }

                m_vChain.push_back(std::move(pNewLink));

            }
            else
            {

                iterPointsToPrevSame->second->m_vChain.push_back(std::move(pNewLink));
            }


            if (iterPointsToPrevSame != mapIndexToDSStructure.end() || (m_lIndex == lIndexPointsToSameValue))
            {
                // 4. Update the other side of the chain, the one that was added to iterPointsToPrevSame
                //
                std::map<tvg_index, std::unique_ptr<AdjacentDStructure>>::iterator iterPrevSameChain = mapIndexToDSStructure.find(pPrevLink->GetIndex());
                if (iterPrevSameChain == mapIndexToDSStructure.end())
                {
                    if (pPrevLink->GetIndex() != tvg_index(-1))
                        _log_root.log("AdjacentDStructure::UpdateOtherNodes 2: while updating chain, failed to update the other link of the inserted link, i.e. update its m_linkLastPointToMe. ID: %ull" +
                                              pPrevSameValue->m_vChain.back()->GetIndex().Get());
                }
                else
                {
                    iterPrevSameChain->second->m_linkLastPointToMe.SetIndex(lIndexPointsToSameValue);
                }
            }
        }


 //   return std::move(pLastAddedDSStructure);
}




AdjacentDStructure::AdjacentDStructure(tvg_index lIndex, tvg_time lConnectionEpoch)
        : m_lIndex(lIndex), m_lConnectionEpoch(lConnectionEpoch),_log_root( _log_root)
{
    m_linkPrevEqualBefore.SetEmpty();
    m_linkPrevEqualAfter.SetEmpty();
    m_linkLastPointToMe.SetEmpty();
}

AdjacentDStructure::~AdjacentDStructure()
{

    std::list<std::unique_ptr<SameNodeNVTemporalLink>>::iterator iter = m_vChain.begin();
    for ( ; iter != m_vChain.end() ; iter++)
        iter->reset();

    m_vChain.clear();

}


tvg_long_counter AdjacentDStructure::GetRealIndexToDSStructureSize()
{
    return (sizeof(AdjacentDStructure) + sizeof(SameNodeNVTemporalLink) * m_vChain.size());
}