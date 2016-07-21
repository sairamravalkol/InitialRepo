//
// Created by norig on 6/21/15.
//


#include <Graph/Header/MyEdge.h>
#include "../Header/MyVertex.h"

#include "../../DRAM/Header/AdjacencyDList.h"
//#include "../../../infrastructure/Configuration.h"

tvg_long_counter MyVertex::m_lIncompleteSize = 0;


MyVertex::MyVertex(tvg_vertex_id iID, tvg_time tEpoch) : m_lID(iID) , m_bReleaseDRAM(false), _log_root(log4cpp::Category::getRoot())
{
    m_lConnectionsCount = 0;
    m_tCreationEpoch = tEpoch;

//    m_pDAdjacencyList = new AdjacencyDList(m_lID);

    m_pDAdjacencyList = std::make_unique<AdjacencyDList>(m_lID);


 //   m_pNVRepository = new NVRepository();

    m_pNVRepository = std::make_unique<NVRepository>(m_lID);


    m_bSubmitted = true;


    m_lCurrentNVLocation = 0;
    m_bNVCompleted = false;

    m_pLastAdded = nullptr;

    m_bSearchMarked = false;


}


MyVertex::~MyVertex()
{
//    if (m_pDAdjacencyList)
//        delete m_pDAdjacencyList;

//    if (m_pNVRepository)
//        delete m_pNVRepository;
}


/*
void MyVertex::UpdateConnection(tvg_vertex_id lID, const std::unique_ptr<NVLink>& pThisNodeLink, const std::unique_ptr<NVLink>& pOtherNodeLink, const tvg_long_counter& lUID, const tvg_time& tEpoch, bool& bCompleteChain)
{
    _log_root.debug("MyVertex::UpdateConnection Start");
//    AdjacencyDList* m_pDAdjacencyList;


    tvg_index indexOfComplete;
    std::unique_ptr<AdjacentDStructure> pCompleteRes = m_pDAdjacencyList->UpdateConnection(tEpoch, m_lConnectionsCount, std::move(m_pLastAdded), lID, indexOfComplete);

    bCompleteChain = (pCompleteRes != NULL);



    // Fill the instant NVRAM data
    //
    m_pLastAdded = m_pNVRepository->UpdateConnection(lUID, pThisNodeLink, pOtherNodeLink, std::move(pCompleteRes), indexOfComplete);


    // const tvg_long_counter& lUID, const std::unique_ptr<NVLink>& pCurrNodeLink,
    // const std::unique_ptr<NVLink>& pOtherNodeLink, std::unique_ptr<AdjacentDStructure> pCompleteRes,
    // const tvg_index& indexOfComplete


    m_lConnectionsCount++;



}
*/

/*
void MyVertex::UpdateConnection(const tvg_long_counter& lUID, std::unique_ptr<NVLink> secondLink, tvg_counter& iCompleteCounter)
{
    _log_root.debug("MyVertex::UpdateConnection Start");

    iCompleteCounter = 0;

    std::map<tvg_long_counter, std::unique_ptr<ReserveStructure>>::iterator iterStoredData = m_mapModificationToData.find(lUID);
    if (iterStoredData == m_mapModificationToData.end())
    {
//        bCompleteChain = false;

        m_mapIdToSecondLink[lUID] = std::move(secondLink);

        _log_root.info("MyVertex::UpdateConnection : Could not find store information for modification number " + std::to_string(lUID));
        return;
    }


    tvg_index indexOfComplete;
    std::unique_ptr<AdjacentDStructure> pCompleteRes = m_pDAdjacencyList->UpdateConnection(iterStoredData->second->GetEpoch(), m_lConnectionsCount, std::move(m_pLastAdded), iterStoredData->second->GetVertexID(), indexOfComplete);

//    bCompleteChain = (pCompleteRes != NULL);

    if (pCompleteRes != nullptr)
    {
        iCompleteCounter = pCompleteRes->GetChain().size();
    }


    // Fill the instant NVRAM data
    //
    m_pLastAdded = m_pNVRepository->UpdateConnection_Optimal(lUID, iterStoredData->second->GetVertexID(), iterStoredData->second->GetIndex(), std::move(secondLink), std::move(pCompleteRes), indexOfComplete);



    m_mapModificationToData.erase(iterStoredData);



    // const tvg_long_counter& lUID, const std::unique_ptr<NVLink>& pCurrNodeLink,
    // const std::unique_ptr<NVLink>& pOtherNodeLink, std::unique_ptr<AdjacentDStructure> pCompleteRes,
    // const tvg_index& indexOfComplete


    m_lConnectionsCount++;
}
*/




// the local version of Reserve
//
// returns if a complete chain was added
//
/*
bool MyVertex::AddLocalConnection(std::unique_ptr<MyVertex>& pV2, const tvg_long_counter& lUID, tvg_time tEpoch, bool bSource, tvg_counter& iFirstCompleteCounter, tvg_counter& iSecondCompleteCounter)
{
    bool bCompleteChain = false;


    //   _log_root.debug("MyVertex::Allocate Start");
    std::unique_ptr<NVLink> pNVLink = std::make_unique<NVLink>(m_lID, m_pNVRepository->Reserve(lUID, tEpoch, pV2->GetID(), bSource));



    std::unique_ptr<ReserveStructure> pNewConnection = std::make_unique<ReserveStructure>(pV2->GetID(), pNVLink->GetIndex(), tEpoch);
    AddToMapModificationToData(lUID, std::move(pNewConnection));


    //
    std::map<tvg_long_counter, std::unique_ptr<NVLink>>::iterator iterUpdate = m_mapIdToSecondLink.find(lUID);
    if (iterUpdate != m_mapIdToSecondLink.end()) // If because of the synchronization the update occurs before the reservation - do not store, just perform both processes
    {
        _log_root.info("MyVertex::Reserve : Update occured before reserve. Node " +  std::to_string(pV2->GetID()) + ", Modification Id: " + std::to_string(lUID));


        UpdateConnection(lUID, std::move(iterUpdate->second), iFirstCompleteCounter);


        m_mapIdToSecondLink.erase(iterUpdate);
    }
    else
    {

    }

    pV2->UpdateConnection(lUID, std::move(pNVLink), iSecondCompleteCounter);


    return bCompleteChain;
}




void MyVertex::AddDistantConnection(std::unique_ptr<VertexProxy>& vp, const tvg_vertex_id& lVertexID, const tvg_long_counter& lUID, tvg_time tEpoch, tvg_vertex_id lNewNodeID, bool bSource, tvg_counter& iFirstCompleteCounter)
{
    //_log_root.warn("MyVertex::AddDistantConnection Start");
    std::unique_ptr<NVLink> pNVLink = std::make_unique<NVLink>(m_lID, m_pNVRepository->Reserve(lUID, tEpoch, lNewNodeID, bSource));



    std::unique_ptr<ReserveStructure> pNewConnection = std::make_unique<ReserveStructure>(pNVLink->GetVertexID(), pNVLink->GetIndex(), tEpoch);
    AddToMapModificationToData(lUID, std::move(pNewConnection));

    //
    std::map<tvg_long_counter, std::unique_ptr<NVLink>>::iterator iterUpdate = m_mapIdToSecondLink.find(lUID);
    if (iterUpdate != m_mapIdToSecondLink.end()) // If because of the synchronization the update occurs before the reservation - do not store, just perform both processes
    {
        _log_root.warn("MyVertex::AddDistantConnection : Update occured before reserve. Node " +  std::to_string(lNewNodeID) + ", Modification Id: " + std::to_string(lUID));


        UpdateConnection(lUID, std::move(iterUpdate->second), iFirstCompleteCounter);


        m_mapIdToSecondLink.erase(iterUpdate);
    }


    vp->PushLink(lVertexID, lUID, pNVLink->GetVertexID(), pNVLink->GetIndex(), tEpoch);
}
*/



tvg_counter MyVertex::AddConnection_Advanced(const tvg_long_counter& lUID, const tvg_vertex_id& lOtherVertexID, tvg_time tEpoch, bool bSource, MODIFICATION_TYPE m)
{
    tvg_index indexOfComplete = -1;
    std::unique_ptr<AdjacentDStructure> pCompleteRes = m_pDAdjacencyList->UpdateConnection(tEpoch, m_lConnectionsCount, std::move(m_pLastAdded), lOtherVertexID, indexOfComplete);

    tvg_counter iChainCounter = 0;
    if (pCompleteRes != nullptr)
    {

        iChainCounter = pCompleteRes->GetChain().size();
    }
    else
    {
        m_lIncompleteSize++;
//        if (m_lIncompleteSize % 1000 == 0)
//            std::cout<<"Total inComplete size = "<<m_lIncompleteSize;
    }

    tvg_counter iSubmittedCount = 0;
    m_pLastAdded = m_pNVRepository->ReserveAndUpdate(lUID, lOtherVertexID, std::move(pCompleteRes), indexOfComplete, tEpoch, bSource, m, iSubmittedCount);


    m_lConnectionsCount++;


    return (iChainCounter - iSubmittedCount);
}




tvg_index MyVertex::Finalize(tvg_index& iChainCounter)
{
    _log_root.debug("MyVertex::Finalize iChainCounter %llu ",iChainCounter.Get());
    iChainCounter = 0;
    tvg_index iAdditionalChainCounter = 0;
    tvg_index iCurrChainCounter;

    tvg_index index;
    std::unique_ptr<AdjacentDStructure> pAdjacentDStructure = m_pDAdjacencyList->GetNextIncomplete(index);
    while (pAdjacentDStructure != NULL)
    {

        iCurrChainCounter = pAdjacentDStructure->GetChain().size();
        iCurrChainCounter = (iCurrChainCounter > NVChainStructure::GetMaxChainSize()) ? (iCurrChainCounter - NVChainStructure::GetMaxChainSize()) : tvg_index(0);
        iAdditionalChainCounter += iCurrChainCounter;


        if (!m_pNVRepository->AddChainStructure(std::move(pAdjacentDStructure), index))
            iChainCounter++;

        pAdjacentDStructure = m_pDAdjacencyList->GetNextIncomplete(index);

    }


    return iAdditionalChainCounter;
}


////std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> MyVertex::LoadIntervalNeighbors_from_db(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch,std::unique_ptr <SendUtils<> > sender) const
////{
////    return std::move(m_pNVRepository->LoadIntervalNeighbors_from_db(tStartEpoch, tEndEpoch));
//}

std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> MyVertex::LoadIntervalNeighbors_Efficient(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch) const
{
    if (m_bSubmitted)
    {
        return std::move(m_pNVRepository->LoadIntervalNeighbors_Efficient_NVRAM(tStartEpoch, tEndEpoch));
    }
    else
    {
        return std::move(m_pNVRepository->LoadIntervalNeighbors_Efficient(tStartEpoch, tEndEpoch));
    }
}

/*
std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> MyVertex::LoadIntervalNeighbors(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch,std::unique_ptr <SendUtils<> > sender) const
{
    return std::move(m_pNVRepository->LoadIntervalNeighbors(tStartEpoch, tEndEpoch));
}
*/



/*
int MyVertex::Compare(MyVertex* v)
{
    _log_root.debug("MyVertex::Compare Start");
    if (m_lID == v->GetID())
        return 0;

    if (m_lID < v->GetID())
        return -1;

    return 1;
}
*/

