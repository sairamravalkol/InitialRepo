//
// Created by norig on 6/24/15.
//

#include <iostream>
#include "../Header/NVRepository.h"
#include "../Header/AdjacentNVVertex.h"
#include <log4cpp/Category.hh>
#include <DRAM/Header/AdjacentDStructure.h>
//#include <RawDataParser.h>
//#include "../../../infrastructure/Configuration.h"
#include "../../../infrastructure/TargetNVM.h"


NVRepository::NVRepository(const tvg_vertex_id& lNodeID):_log_root(log4cpp::Category::getRoot()), m_lVertecesRemovedSize(0)/*, m_lChainRemovedSize(0)*/, m_lNodeID(lNodeID)
{
    m_bUnloaded = false;

    ResetAll();
    ResetVertecesAndChains();
//    ResetVerteces();
//    ResetChain();
    ResetAdditionalChain();
    ResetMapAdditionalNodesToChains();



    m_linkEmpty = std::make_unique<SameNodeNVLink>();
}



void NVRepository::ResetAll()
{

    m_lAdditionalHashSize = 0;


    m_lVertecesDoubleSize = 0;
    m_lVertecesCurrCompletedSize = 0;
//    m_lVertecesReservedCurrSize = 0;
    m_lVertecesRemovedSize = 0;

/*
    m_lChainDoubleSize = 0;
    m_lChainCurrSize = 0;
    m_lChainRemovedSize = 0;
*/


    m_lAdditionalChainDoubleLength = 0;
    m_lAdditionalChainCurrLength = 0;
    m_lAdditionalChainRemovedLength = 0;

    m_lSavedVertecesCurrCompletedSize = 0;
    m_lSavedVertecesRemovedSize = 0;
//    m_lSavedChainRemovedSize = 0;
//    m_lSavedChainCurrSize = 0;
    m_lSavedAdditionalChainRemovedLength = 0;
    m_lSavedAdditionalChainCurrLength = 0;

    m_lSavedAdditionalHashSize = 0;

}

void NVRepository::ResetVertecesAndChains(tvg_index lInitialLength)
{
    //log4cpp::Category::getRoot().warn("NVRepository::ResetVertecesAndChains reset to initial "+std::to_string(lInitialLength)+" complete size was "+std::to_string(m_lVertecesCurrCompletedSize)+" double size was " + std::to_string(m_lVertecesDoubleSize));
    m_lSavedVertecesCurrCompletedSize += m_lVertecesCurrCompletedSize;
    m_lSavedVertecesRemovedSize += m_lVertecesRemovedSize;

    m_lVertecesRemovedSize = 0;
//    m_lVertecesReservedCurrSize -= m_lVertecesCurrCompletedSize;


    m_lVertecesCurrCompletedSize = 0;

    m_lVertecesDoubleSize = lInitialLength;

    m_aNVAdjacencyList = std::make_unique<AdjacentNVVertex[]>(lInitialLength);
    m_aChainStructures = std::make_unique<NVChainStructure[]>(lInitialLength);//TODO - need to init smartly and not by allocating and releasing

}


/*
void NVRepository::ResetVerteces(tvg_index lInitialLength)
{
    m_lSavedVertecesCurrCompletedSize += m_lVertecesCurrCompletedSize;
    m_lSavedVertecesRemovedSize += m_lVertecesRemovedSize;

    m_lVertecesRemovedSize = 0;
//    m_lVertecesReservedCurrSize -= m_lVertecesCurrCompletedSize;


    m_lVertecesCurrCompletedSize = 0;

    m_lVertecesDoubleSize = lInitialLength;

    m_aNVAdjacencyList = std::make_unique<AdjacentNVVertex[]>(lInitialLength);


}



void NVRepository::ResetChain(tvg_index lInitialLength)
{
    m_lSavedChainRemovedSize += m_lChainRemovedSize;
    m_lSavedChainCurrSize += m_lChainCurrSize;


    m_lChainRemovedSize = 0;
    m_lChainCurrSize = 0;
    m_lChainDoubleSize = lInitialLength;

    m_aChainStructures = std::make_unique<NVChainStructure[]>(lInitialLength);//TODO - need to init smartly and not by allocating and releasing
}
*/

void NVRepository::ResetAdditionalChain(tvg_index lAdditionalChainInitialLength)
{
    m_lSavedAdditionalChainRemovedLength += m_lAdditionalChainRemovedLength;
    m_lSavedAdditionalChainCurrLength += m_lAdditionalChainCurrLength;

    m_lAdditionalChainRemovedLength = 0;
    m_lAdditionalChainCurrLength = 0;
    m_lAdditionalChainDoubleLength = lAdditionalChainInitialLength;

    m_aAdditionalChains = std::make_unique<SameNodeNVTemporalLink[]>(lAdditionalChainInitialLength);
}


void NVRepository::ResetMapAdditionalNodesToChains()
{
    m_lSavedAdditionalHashSize += m_lAdditionalHashSize;
    m_lAdditionalHashSize = 0;

    m_mapAdditionalNodesToChains = std::make_unique<std::map<SameNodeNVLink, NVIntervalLink>>();
}


NVRepository::~NVRepository()
{
//    if (m_aNVAdjacencyList)
//        delete [] m_aNVAdjacencyList;


//    if (m_aChains)
//        delete [] m_aChains;


}



// chains only
//
void NVRepository::CompleteSetData(const tvg_vertex_id& lID, const tvg_index& lIndex, std::list<std::unique_ptr<SameNodeNVTemporalLink>>& lLinks, int iCounter, std::list<std::unique_ptr<SameNodeNVTemporalLink>>::iterator iterLinks)
{
    SameNodeNVLink linkAdditionalChain(lIndex);
    NVIntervalLink linkAdditionalChainLocation(m_lAdditionalChainCurrLength, lLinks.size() - iCounter);
    (*m_mapAdditionalNodesToChains)[linkAdditionalChain] = linkAdditionalChainLocation;
    m_lAdditionalHashSize++;

    for (; iterLinks != lLinks.end(); iterLinks++)
    {
        std::unique_ptr<SameNodeNVTemporalLink> pLink = std::make_unique<SameNodeNVTemporalLink>((*iterLinks)->GetIndex(), (*iterLinks)->GetEpoch());
        m_aAdditionalChains[m_lAdditionalChainCurrLength].SetLink(std::move(pLink));

        m_lAdditionalChainCurrLength++;
        if (m_lAdditionalChainCurrLength >= m_lAdditionalChainDoubleLength)
        {
            DoubleAdditionalChain();
        }
    }
}


bool NVRepository::AddChainStructure(std::unique_ptr<AdjacentDStructure> pAdjacentDStructure, const tvg_index& indexOfComplete)
{
    bool bSubmitted;
    if (indexOfComplete >= m_lSavedVertecesCurrCompletedSize)
    {
        bSubmitted = false;
        m_aChainStructures[indexOfComplete - m_lSavedVertecesCurrCompletedSize].SetData(std::move(pAdjacentDStructure), m_lNodeID, *this);
    }
    else
    {

        //////////////////
        bSubmitted = true;

        std::unique_ptr<NVChainStructure> newChainVal = std::make_unique<NVChainStructure>();
        newChainVal->SetData(std::move(pAdjacentDStructure), m_lNodeID, *this);

        BufferManager<TargetNVM>::Instance()->FixSubmitVertex(m_lNodeID, indexOfComplete, std::move(newChainVal));
    }

//    m_lVertecesCurrCompletedSize++;


    return bSubmitted;
}



tvg_index NVRepository::GetSuccessorInAdditionalLinks(const tvg_index& lCurr, const tvg_time& tEndEpoch) // probably need to perform a binary search - later
{
    tvg_index indexResult;
    SameNodeNVLink nvLink(lCurr);

    std::map<SameNodeNVLink, NVIntervalLink>::iterator iter = m_mapAdditionalNodesToChains->find(nvLink);
    if (iter == m_mapAdditionalNodesToChains->end())
    {
        _log_root.error("NVRepository::GetSuccessorInAdditionalLinks(): cannot find element on index = " + std::to_string(lCurr)); // exception
        return indexResult;
    }

    for (int i = iter->second.GetIndex() ; i < iter->second.GetIndex() + iter->second.GetSize() ; i++)
    {
        if (m_aAdditionalChains[i].GetEpoch() > tEndEpoch)
        {
            if (i > 0)
                return m_aAdditionalChains[i-1].GetIndex();
            else
                return -1;
        }
    }


    return m_aAdditionalChains[iter->second.GetSize()-tvg_index(1)].GetIndex();
}




tvg_index NVRepository::GetSuccessorInAdditionalLinks_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch) // probably need to perform a binary search - later
{
    if (m_mapAdditionalNodesToChains->empty())
        LoadMapAdditionalNodesToChains();

    tvg_index indexResult;
    SameNodeNVLink nvLink(lCurr);

    std::map<SameNodeNVLink, NVIntervalLink>::iterator iter = m_mapAdditionalNodesToChains->find(nvLink);
    if (iter == m_mapAdditionalNodesToChains->end())
    {
        _log_root.error("NVRepository::GetSuccessorInAdditionalLinks(): cannot find element on index = " + std::to_string(lCurr)); // exception
        return indexResult;
    }


    std::unique_ptr<SameNodeNVTemporalLink[]> nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<SameNodeNVTemporalLink>(m_lNodeID, iter->second.GetIndex(), iter->second.GetSize()));

    for (int i = 0 ; i < iter->second.GetSize() ; i++)
    {
        if (nvData[i].GetEpoch() > tEndEpoch)
        {
            if (i > 0)
                return nvData[i-1].GetIndex();
            else
                return -1;
        }
    }


    return nvData[iter->second.GetSize()-tvg_index(1)].GetIndex();
}




tvg_index NVRepository::LoadNext(const tvg_index& lCurr, const tvg_time& tEndEpoch)
{
    return m_aChainStructures[lCurr].GetSuccessor(lCurr, tEndEpoch, *this);
}



tvg_index NVRepository::LoadNext_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch)
{
    std::unique_ptr<NVChainStructure[]> nvData;
    try
    {
        nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<NVChainStructure>(m_lNodeID, lCurr, 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::LoadNext_NVRAM() ERROR: could not load NVChainStructure data of node " + lCurr.GetString() + ", index = " + std::to_string(0));
        return tvg_index(-1);
    }



    return nvData[0].GetSuccessor_NVRAM(lCurr, tEndEpoch, *this);
}


//////////////// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// meanwhile problem: the links structure can be damaged in case we add values in the wrong order
/*
std::unique_ptr<AdjacentNVVertex> NVRepository::UpdateConnection_Optimal(const tvg_long_counter& lUID,
                                                                 const tvg_vertex_id& lThisVertexID, const tvg_index& lThisVertexIndex,
                                                                 std::unique_ptr<NVLink> pSecondLink,
                                                                 std::unique_ptr<AdjacentDStructure> pCompleteRes,
                                                                 const tvg_index& indexOfComplete)
{
    // find the right location to add the item
    //
    tvg_index indexAddition;
    std::map<tvg_long_counter, tvg_index>::iterator iterLocation = m_mapIncompleteModificationToIndex.find(lUID);
    if (iterLocation == m_mapIncompleteModificationToIndex.end()) // cuurently, this should never happen, since the existence of the reservation should be verified in MyVertex call.
    {
        _log_root.error("NVRepository::UpdateConnection(): no allocation for insertion of UID = " + std::to_string(lUID)); // exception
        indexAddition = m_lVertecesReservedCurrSize;
        m_lVertecesReservedCurrSize++;
    }
    else
    {
        indexAddition = iterLocation->second;
        m_mapIncompleteModificationToIndex.erase(iterLocation);
    }


    m_aNVAdjacencyList[indexAddition].SetStructureData(lThisVertexID, lThisVertexIndex, pSecondLink->GetVertexID(), pSecondLink->GetIndex());

    // The copy is since the NVRAM data might be removed
    //
    std::unique_ptr<AdjacentNVVertex> pCopyElement = std::make_unique<AdjacentNVVertex>(m_aNVAdjacencyList[indexAddition]);



    // Fill the instant NVRAM data
    //
    if (indexAddition != m_lVertecesCurrCompletedSize)
    {
        _log_root.error("NVRepository::UpdateConnection(): indexAddition != m_lVertecesCurrSize while inserting UID = " + std::to_string(lUID)); // exception
    }
    else
    {
        // Only now update the link structure if the next is in order
        //


        m_lChainCurrSize++;
        if (m_lChainCurrSize >= m_lChainDoubleSize)
        {
            DoubleChain();
        }


        m_lVertecesCurrCompletedSize++;


        // aDD COMPLETE, IF EXISTS
        //
        if (pCompleteRes != NULL)
        {
            m_aChainStructures[indexOfComplete].SetData(std::move(pCompleteRes), m_lNodeID, *this);
        }

    }



    return std::move(pCopyElement);
}
*/


std::unique_ptr<AdjacentNVVertex> NVRepository::ReserveAndUpdate(const tvg_long_counter& lUID, const tvg_vertex_id& lOtherVertexID,
                                                                 std::unique_ptr<AdjacentDStructure> pCompleteRes, const tvg_index& indexOfComplete,
                                                                 tvg_time tEpoch, bool bSource, MODIFICATION_TYPE m, tvg_counter& iSubmittedCount)
{
    m_aNVAdjacencyList[m_lVertecesCurrCompletedSize].SetData(lUID, tEpoch, m_lSavedVertecesCurrCompletedSize + m_lVertecesCurrCompletedSize, lOtherVertexID, bSource, indexOfComplete, m);




//    return (m_lVertecesCurrCompletedSize - tvg_index(1));


    ///////////////////////////////////////////////////////////////////////////////////////





    // Part of the old version, where sending the cross links
    //
//    m_aNVAdjacencyList[m_lVertecesCurrCompletedSize].SetStructureData(m_lNodeID, m_lSavedVertecesCurrCompletedSize + m_lVertecesCurrCompletedSize, m_linkEmpty->GetVertexID(), m_linkEmpty->GetIndex());



    // The copy is since the NVRAM data might be removed
    //
    std::unique_ptr<AdjacentNVVertex> pCopyElement = std::make_unique<AdjacentNVVertex>(m_aNVAdjacencyList[m_lVertecesCurrCompletedSize]);





//    m_lChainCurrSize++;
//    if (m_lChainCurrSize >= m_lChainDoubleSize)
//    {
//        DoubleChain();
//    }




    // aDD COMPLETE, IF EXISTS
    //
    if (pCompleteRes != NULL)
    {
        if (indexOfComplete >= m_lSavedVertecesCurrCompletedSize)
        {
            m_aChainStructures[indexOfComplete - m_lSavedVertecesCurrCompletedSize].SetData(std::move(pCompleteRes), m_lNodeID, *this);
        }
        else
        {

            //////////////////

            std::unique_ptr<NVChainStructure> newChainVal = std::make_unique<NVChainStructure>();
            newChainVal->SetData(std::move(pCompleteRes), m_lNodeID, *this);

            BufferManager<TargetNVM>::Instance()->FixSubmitVertex(m_lNodeID, indexOfComplete, std::move(newChainVal));

            iSubmittedCount++;
        }
    }



    m_lVertecesCurrCompletedSize++;
//    m_lVertecesReservedCurrSize++;
    if (m_lVertecesCurrCompletedSize >= m_lVertecesDoubleSize)
    {
        DoubleVerticesAndChain();
    }


    return std::move(pCopyElement);
}


/*
std::unique_ptr<AdjacentNVVertex> NVRepository::UpdateConnection_Advanced(const tvg_long_counter& lUID,
                                                                 const tvg_vertex_id& lThisVertexID, const tvg_index& lThisVertexIndex,
                                                                 std::unique_ptr<AdjacentDStructure> pCompleteRes,
                                                                 const tvg_index& indexOfComplete)
{
    // find the right location to add the item
    //
    tvg_index indexAddition;
    std::map<tvg_long_counter, tvg_index>::iterator iterLocation = m_mapIncompleteModificationToIndex.find(lUID);
    if (iterLocation == m_mapIncompleteModificationToIndex.end()) // cuurently, this should never happen, since the existence of the reservation should be verified in MyVertex call.
    {
        _log_root.error("NVRepository::UpdateConnection(): no allocation for insertion of UID = " + std::to_string(lUID)); // exception
        indexAddition = m_lVertecesReservedCurrSize;
        m_lVertecesReservedCurrSize++;
    }
    else
    {
        indexAddition = iterLocation->second;
        m_mapIncompleteModificationToIndex.erase(iterLocation);
    }


    m_aNVAdjacencyList[indexAddition].SetStructureData(lThisVertexID, lThisVertexIndex, m_linkEmpty->GetVertexID(), m_linkEmpty->GetIndex());

    // The copy is since the NVRAM data might be removed
    //
    std::unique_ptr<AdjacentNVVertex> pCopyElement = std::make_unique<AdjacentNVVertex>(m_aNVAdjacencyList[indexAddition]);



    // Fill the instant NVRAM data
    //
    if (indexAddition != m_lVertecesCurrCompletedSize)
    {
        _log_root.error("NVRepository::UpdateConnection(): indexAddition != m_lVertecesCurrSize while inserting UID = " + std::to_string(lUID)); // exception
    }
    else
    {
        // Only now update the link structure if the next is in order
        //


        m_lChainCurrSize++;
        if (m_lChainCurrSize >= m_lChainDoubleSize)
        {
            DoubleChain();
        }


        m_lVertecesCurrCompletedSize++;


        // aDD COMPLETE, IF EXISTS
        //
        if (pCompleteRes != NULL)
        {
            m_aChainStructures[indexOfComplete].SetData(std::move(pCompleteRes), m_lNodeID, *this);
        }

    }



    return std::move(pCopyElement);
}
*/

/*
tvg_index NVRepository::Reserve(const tvg_long_counter& lUID, tvg_time tEpoch, tvg_vertex_id lNewNodeID, bool bSource)
{
    m_mapIncompleteModificationToIndex[lUID] = m_lVertecesReservedCurrSize;

    m_aNVAdjacencyList[m_lVertecesReservedCurrSize].SetAdjData(lUID, tEpoch, m_lVertecesReservedCurrSize, lNewNodeID, bSource);

    m_lVertecesReservedCurrSize++;
    if (m_lVertecesReservedCurrSize >= m_lVertecesDoubleSize)
    {
        DoubleVertices();
    }


    return (m_lVertecesReservedCurrSize - tvg_index(1));
}
*/






//std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> NVRepository::LoadIntervalNeighbors_from_db(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch)
//{
//    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> vOutput = std::make_unique<std::vector<std::unique_ptr<NeighborMetaData>>>();
//
//    bool isVertica = false;
//    isVertica = Configuration::Instance()->GetStringItem("database.source_type").find("vertica") != std::string::npos;
//    auto source = std::unique_ptr<DBSource>();
//    if(isVertica)
//    {
//
//        source = std::move(std::unique_ptr<DBSource>(new DBVerticaSource()));
//    }
//    else
//    {
//        source = std::move(std::unique_ptr<DBSource>(new DBSQLiteSource()));
//    }
//
//    source->Init();
//    auto neighbors_in_db  = source->GetNeighbors(m_lNodeID,tStartEpoch,tEndEpoch);
//    for(auto& nbr : neighbors_in_db)
//    {
//        std::unique_ptr<NeighborMetaData> newValue = std::make_unique<NeighborMetaData>(nbr,-1, 1);
//        vOutput->push_back(std::move(newValue));
//    }
//    source->Finalize();
//    return std::move(vOutput);
//
//}
//////////////////////////// End LoadsNext
//




//////////////////////////// BeginLoad intervals
//
std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> NVRepository::LoadIntervalNeighbors_Efficient(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch)
{
    std::unique_ptr<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>> hID2Freq;



    // Binary search for the first value
    //
    tvg_index lIndex = BinarySearchPoint(tEndEpoch, 0, m_lVertecesCurrCompletedSize - tvg_index(1));


    if (lIndex.IsValid())
    {
        std::map<tvg_vertex_id, std::pair<tvg_counter, tvg_time>>::iterator iter;
        tvg_vertex_id lID;


        hID2Freq = std::make_unique<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>>();


        tvg_index l = lIndex;
        while ((l.IsValid()) && (m_aNVAdjacencyList[l].GetEpoch() >= tStartEpoch))
        {
            _log_root.error("NVRepository::LoadIntervalNeighbors_Efficient in there");
            lID = m_aNVAdjacencyList[l].GetID();

            std::unique_ptr<NeighborMetaData> pNewNeighborMetaData = std::make_unique<NeighborMetaData>(lID, m_aNVAdjacencyList[l].GetEpoch(), 1); // (const tvg_time& tEpoch, const tvg_counter& lCounter)
            (*hID2Freq)[lID] = std::move(pNewNeighborMetaData);


            l = LoadNext(l, tEndEpoch);
        }
    }


    // Constract the output as a vector
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> vOutput;

    if ((hID2Freq  != NULL) && (!hID2Freq->empty()))
    {
        vOutput = std::make_unique<std::vector<std::unique_ptr<NeighborMetaData>>>(hID2Freq->size());

        std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterOut = hID2Freq->begin();
        for (int i = 0; iterOut != hID2Freq->end(); i++, iterOut++)
        {
            (*vOutput)[i] = std::move(iterOut->second);
        }
    }
    return std::move(vOutput);
}



void NVRepository::LoadMetadata()
{


    if (m_bUnloaded)
    {
        std::unique_ptr<tvg_index[]> sermetadata = std::move(BufferManager<TargetNVM>::Instance()->Get<tvg_index>(m_lNodeID, 0, GetCurrMetadataSize()));

        m_lSavedVertecesCurrCompletedSize = sermetadata[0];
        m_lSavedVertecesRemovedSize = sermetadata[1];
//        m_lSavedChainRemovedSize = sermetadata[2];
//        m_lSavedChainCurrSize = sermetadata[3];
        m_lSavedAdditionalChainRemovedLength = sermetadata[2];
        m_lSavedAdditionalChainCurrLength = sermetadata[3];
        m_lSavedAdditionalHashSize = sermetadata[4];

        m_bUnloaded = false;
    }




    m_lVertecesCurrCompletedSize = m_lSavedVertecesCurrCompletedSize;
    m_lVertecesRemovedSize = m_lSavedVertecesRemovedSize;

 //   m_lChainRemovedSize = m_lSavedChainRemovedSize;
 //   m_lChainCurrSize = m_lSavedChainCurrSize;

    m_lAdditionalChainRemovedLength = m_lSavedAdditionalChainRemovedLength;
    m_lAdditionalChainCurrLength = m_lSavedAdditionalChainCurrLength;

    m_lAdditionalHashSize = m_lSavedAdditionalHashSize;
}




std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> NVRepository::LoadIntervalNeighbors_Efficient_NVRAM(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch)
{

    LoadMetadata();

    std::unique_ptr<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>> hID2Freq;

    tvg_index lIndex;
    // Binary search for the first value
    //


    std::unique_ptr<AdjacentNVVertex[]> nvData = std::move(
            BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lNodeID,
                                                                        m_lVertecesCurrCompletedSize - tvg_index(1),
                                                                        1));


    lIndex = BinarySearchPoint_NVRAM(m_lNodeID, tEndEpoch, 0, m_lVertecesCurrCompletedSize - tvg_index(1), nvData[0]);


    if (lIndex.IsValid())
    {
        std::map<tvg_vertex_id, std::pair<tvg_counter, tvg_time>>::iterator iter;
        tvg_vertex_id lID;


        hID2Freq = std::make_unique<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>>();


        tvg_index l = lIndex;

        do
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lNodeID, l, 1)); // Optimization latter, no need to recall it in case the last value is correct. .

            lID = nvData[0].GetID();

            std::unique_ptr<NeighborMetaData> pNewNeighborMetaData = std::make_unique<NeighborMetaData>(lID, nvData[0].GetEpoch(), 1);
            (*hID2Freq)[lID] = std::move(pNewNeighborMetaData);


            l = LoadNext_NVRAM(l, tEndEpoch);
        }
        while ((nvData != nullptr) && (l.IsValid()) && (nvData[0].GetEpoch() >= tStartEpoch));
    }
    // Constract the output as a vector
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> vOutput;

    if ((hID2Freq  != NULL) && (!hID2Freq->empty()))
    {
        vOutput = std::make_unique<std::vector<std::unique_ptr<NeighborMetaData>>>(hID2Freq->size());

        std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterOut = hID2Freq->begin();
        for (int i = 0; iterOut != hID2Freq->end(); i++, iterOut++)
        {
            (*vOutput)[i] = std::move(iterOut->second);
        }
    }

    return std::move(vOutput);
}







std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> NVRepository::LoadIntervalNeighbors(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch)
{
    std::unique_ptr<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>> hID2Freq;

    // Binary search for the first value
    //
    tvg_index lIndex = BinarySearchPoint(tEndEpoch, 0, m_lVertecesCurrCompletedSize - tvg_index(1));

    if (lIndex.IsValid())
    {
        std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iter;
        tvg_vertex_id lID;


        hID2Freq = std::make_unique<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>>();

        tvg_index l = lIndex;
        for ( ; (l.IsValid()) && (m_aNVAdjacencyList[l].GetEpoch() >= tStartEpoch) ; l--)
        {
 //           if (!m_aNVAdjacencyList[l].IsSource())
 //               continue;

            lID = m_aNVAdjacencyList[l].GetID();
            unsigned long long lDebug = lID.Get();

            iter = hID2Freq->find(lID);
            if (iter == hID2Freq->end())
            {
                std::unique_ptr<NeighborMetaData> newValue = std::make_unique<NeighborMetaData>(lID, m_aNVAdjacencyList[l].GetEpoch(), 1);
                (*hID2Freq)[lID] = std::move(newValue); /// (const NVLink& location, const tvg_time& tEpoch, const tvg_counter& lCounter)
            }
            else
            {
                iter->second->IncreaseCount();
 //               auto record = iter->second;
//                (*hID2Freq)[lID] = std::make_pair(std::get<0>(record) + 1, std::get<1>(record));
            }

        }
    }



    // Constract the output as a vector
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> vOutput;

    if ((hID2Freq != NULL ) && (!hID2Freq->empty()))
    {
        vOutput = std::make_unique<std::vector<std::unique_ptr<NeighborMetaData>>>(hID2Freq->size());

        std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterOut = hID2Freq->begin();
        for (int i = 0; iterOut != hID2Freq->end(); i++, iterOut++) {
            (*vOutput)[i] = std::move(iterOut->second);
        }
    }

    return std::move(vOutput);
}

//////////////////////////// End Load intervals
//


//////////////////////////// Begin Binary search
//
tvg_index NVRepository::BinarySearchPoint(const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast)
{
    if ((!lFirst.IsValid()) || (!lLast.IsValid()) || (lFirst > lLast))
        return -1;

    else
    {
        tvg_index lMid = (lFirst + lLast)/tvg_index(2);
        if (lMid == lFirst)
        {
            if (lMid == lLast)
            {
                if (lMid > tvg_index(0) && (tEpoch < m_aNVAdjacencyList[lMid].GetEpoch()))
                    return (lMid - tvg_index(1));
                else
                    return lMid;
            }

            else
            {
                if (tEpoch >= m_aNVAdjacencyList[lLast].GetEpoch())
                {
                    return lLast;
                }
                else
                {
                    if (tEpoch < m_aNVAdjacencyList[lMid].GetEpoch())
                        return lMid-tvg_index(1); // -1
                    else
                        return lMid;
                }
            }
        }
        else
        {
            if (tEpoch == m_aNVAdjacencyList[lMid].GetEpoch())
                return BinarySearchPoint(tEpoch, lMid, lLast);
            else
            {
                if (tEpoch < m_aNVAdjacencyList[lMid].GetEpoch())
                    return BinarySearchPoint(tEpoch, lFirst, lMid - tvg_index(1));
                else
                    return BinarySearchPoint(tEpoch, lMid + tvg_index(1), lLast);
            }
        }
    }
}





tvg_index NVRepository::BinarySearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast, AdjacentNVVertex nvLastValue)
{
    if ((!lFirst.IsValid()) || (!lLast.IsValid()) || (lFirst > lLast))
        return -1;

    else
    {
        tvg_index lMid = (lFirst + lLast)/tvg_index(2);
        std::unique_ptr<AdjacentNVVertex[]> nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid, 1));

        if (lMid == lFirst)
        {
            if (lMid == lLast)
            {
                if (lMid > tvg_index(0) && (tEpoch < nvData[0].GetEpoch()))
                    return (lMid - tvg_index(1));
                else
                    return lMid;
            }

            else
            {
                if (tEpoch >= nvLastValue.GetEpoch())
                {
                    return lLast;
                }
                else
                {
                    if (tEpoch < nvData[0].GetEpoch())
                        return lMid-tvg_index(1); // -1
                    else
                        return lMid;
                }
            }
        }
        else
        {
            if (tEpoch == nvData[0].GetEpoch())
                return BinarySearchPoint_NVRAM(lID, tEpoch, lMid, lLast, nvLastValue);
            else
            {
                if (tEpoch < nvData[0].GetEpoch())
                {
                    std::unique_ptr<AdjacentNVVertex[]> nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid - tvg_index(1), 1));
                    return BinarySearchPoint_NVRAM(lID, tEpoch, lFirst, lMid - tvg_index(1), nvData[0]);
                }
                else
                    return BinarySearchPoint_NVRAM(lID, tEpoch, lMid + tvg_index(1), lLast, nvLastValue);
            }
        }
    }
}

//////////////////////////// End Binary search
//


//////////////////////////// Begin Doubles
//
void NVRepository::DoubleVerticesAndChain()
{
    if (m_lVertecesCurrCompletedSize < m_lVertecesDoubleSize)
    {
        _log_root.error("NVRepository::DoubleVerticesandChain(): wrong call for double"); // exception
        return;
    }
    log4cpp::Category::getRoot().debug("need to allocate new space for repository vertices!!");


    // The list
    //
    /*
    AdjacentNVVertex* pNewNVAdjacencyList = new AdjacentNVVertex[m_lAllocatedSize * 2];
    for (tvg_index i = 0 ; i < m_lAllocatedSize ; i++)
    {
        pNewNVAdjacencyList[i] = m_aNVAdjacencyList[i];

    }
    delete [] m_aNVAdjacencyList;
    m_aNVAdjacencyList = pNewNVAdjacencyList;
*/

    std::unique_ptr<AdjacentNVVertex[]> m_aNVAdjacencyList2 = std::move(m_aNVAdjacencyList);
    std::unique_ptr<NVChainStructure[]> m_aChainStructures2 = std::move(m_aChainStructures);

    m_lVertecesDoubleSize *= tvg_index(2);

    m_aNVAdjacencyList = std::make_unique<AdjacentNVVertex[]>(m_lVertecesDoubleSize);
    m_aChainStructures = std::make_unique<NVChainStructure[]>(m_lVertecesDoubleSize);

    for (tvg_index i = 0 ; i < m_lVertecesCurrCompletedSize ; i++)
    {
        m_aNVAdjacencyList[i] = m_aNVAdjacencyList2[i];
        m_aChainStructures[i] = m_aChainStructures2[i];
    }
}

/*
void NVRepository::DoubleChain()
{
    if (m_lChainCurrSize < m_lChainDoubleSize)
    {
        _log_root.error("NVRepository::DoubleChain(): wrong call for double"); // exception
        return;
    }
    log4cpp::Category::getRoot().info("need to allocate new space for repository chain !!");




    std::unique_ptr<NVChainStructure[]> m_aChainStructures2 = std::move(m_aChainStructures);
    m_aChainStructures = std::make_unique<NVChainStructure[]>(m_lChainDoubleSize * tvg_index(2));
    for (tvg_index i = 0 ; i < m_lChainCurrSize ; i++)
    {
        m_aChainStructures[i] = m_aChainStructures2[i];

    }


    m_lChainDoubleSize *= tvg_index(2);

}
*/

void NVRepository::DoubleAdditionalChain()
{

    if (m_lAdditionalChainCurrLength < m_lAdditionalChainDoubleLength)
    {
        _log_root.error("NVRepository::DoubleAdditionalChain(): wrong call for double"); // exception
        return;
    }
    log4cpp::Category::getRoot().debug("need to allocate new space for repository chain !!");



    std::unique_ptr<SameNodeNVTemporalLink[]> m_aAdditionalChains2 = std::move(m_aAdditionalChains);
    m_aAdditionalChains = std::make_unique<SameNodeNVTemporalLink[]>(m_lAdditionalChainDoubleLength * tvg_index(2));
    for (tvg_index i = 0 ; i < m_lAdditionalChainCurrLength ; i++)
    {
        m_aAdditionalChains[i] = m_aAdditionalChains2[i];

    }

    m_lAdditionalChainDoubleLength *= tvg_index(2);
}

//////////////////////////// End Doubles
//





// evict the repository to the NVM
void NVRepository::Evict()
{
    //lock the data
    //move the m_aNVAdjacencyList to the NVM region that corresponds to this vertex

    //move the m_aChains to the nvm

    //record the fact that the current data was moved and to which offset
}



void NVRepository::LoadMapAdditionalNodesToChains()
{
    std::unique_ptr<NVLinkPairInterval[]> nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<NVLinkPairInterval>(m_lNodeID, 0, m_lAdditionalHashSize));

    m_mapAdditionalNodesToChains = std::make_unique<std::map<SameNodeNVLink, NVIntervalLink>>();
    for (int i = 0 ; i <  m_lAdditionalHashSize ; i++)
    {
        (*m_mapAdditionalNodesToChains)[nvData[i].GetFirstLink()] = nvData[i].GetSecondLink();
    }
}


std::unique_ptr<NVChainStructure[]>& NVRepository::GetChains()
{
    return m_aChainStructures;
}


std::unique_ptr<AdjacentNVVertex[]> NVRepository::SubmitNVAdjacencyList()
{
    std::unique_ptr<AdjacentNVVertex[]> m_aNVAdjacencyList2 = std::move(m_aNVAdjacencyList);
//    ResetVerteces();
    return std::move(m_aNVAdjacencyList2);
}

std::unique_ptr<NVChainStructure[]> NVRepository::SubmitChains()
{
    std::unique_ptr<NVChainStructure[]> m_aChainStructures2 = std::move(m_aChainStructures);
//    ResetChain();
//    ResetVertecesAndChains();
    return std::move(m_aChainStructures2);
}

std::unique_ptr<SameNodeNVTemporalLink[]> NVRepository::SubmitAdditionalChains()
{
    std::unique_ptr<SameNodeNVTemporalLink[]> m_aAdditionalChains2 = std::move(m_aAdditionalChains);
    ResetAdditionalChain();
    return std::move(m_aAdditionalChains2);
}

std::unique_ptr<NVLinkPairInterval[]> NVRepository::SubmitMapAdditionalNodesToChains()
{
    std::unique_ptr<NVLinkPairInterval[]> serMapAdditionalNodesToChains = std::make_unique<NVLinkPairInterval[]>(m_mapAdditionalNodesToChains->size());
    std::map<SameNodeNVLink, NVIntervalLink>::iterator iter = m_mapAdditionalNodesToChains->begin();
    for (int i = 0 ; iter != m_mapAdditionalNodesToChains->end() ; iter++, i++)
    {
        serMapAdditionalNodesToChains[i].SetData(iter->first, iter->second);
    }
    ResetMapAdditionalNodesToChains();
    return std::move(serMapAdditionalNodesToChains);
}

std::unique_ptr<tvg_index[]> NVRepository::SubmitMetadata()
{
    std::unique_ptr<tvg_index[]> sermetadata = std::make_unique<tvg_index[]>(GetCurrMetadataSize());

    sermetadata[0] = m_lSavedVertecesCurrCompletedSize;
    sermetadata[1] = m_lSavedVertecesRemovedSize;
//    sermetadata[2] = m_lSavedChainRemovedSize;
//    sermetadata[3] = m_lSavedChainCurrSize;
    sermetadata[2] = m_lSavedAdditionalChainRemovedLength;
    sermetadata[3] = m_lSavedAdditionalChainCurrLength;
    sermetadata[4] = m_lSavedAdditionalHashSize;

    ResetAll();
    m_bUnloaded = true;

    return std::move(sermetadata);
}

tvg_index NVRepository::GetCurrMetadataSize()
{
    return 5;
}

tvg_index NVRepository::GetCompletedVerticesNVLocation()
{
    return m_lVertecesCurrCompletedSize;
}
/*
tvg_index NVRepository::GetCurrChainElementsCount()
{
    return m_lChainCurrSize;
}
*/
tvg_index NVRepository::GetCurrAdditionalChainElementsCount()
{
    return m_lAdditionalChainCurrLength;
}


tvg_index NVRepository::GetCurrMapAdditionalNodesToChainsElementsCount()
{
    if (m_lAdditionalHashSize == m_mapAdditionalNodesToChains->size())
        return m_lAdditionalHashSize;
    else
    {
        _log_root.error("NVRepository::GetCurrMapAdditionalNodesToChainsElementsCount(): the size of the hash = " + std::to_string(m_mapAdditionalNodesToChains->size()) +
                        "is different from the size m_lAdditionalHashSize = " + std::to_string(m_lAdditionalHashSize)); // exception

        return m_mapAdditionalNodesToChains->size();
    }
}
