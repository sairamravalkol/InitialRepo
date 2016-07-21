//
// Created by norig on 6/24/15.
//

#ifndef TVG_IGOR_NVREPOSITORY_H
#define TVG_IGOR_NVREPOSITORY_H


//#include "NVChainStructure.h"
#include <log4cpp/Category.hh>
#include "../../DRAM/Header/AdjacentDStructure.h"
#include "../../../infrastructure/BufferManager.h"

#include "ChainAdditionalData.h"


#include "AdjacentNVVertex.h"
//#include "NVChainStructure.h"
//#include <log4cpp/Category.hh>

//#include "../../Graph/Header/GraphMemoryManager.h"
#include "../../General/Header/tvg_index.h"
#include "../../General/Header/tvg_vertex_id.h"
#include "../../General/Header/tvg_time.h"
#include "../../General/Header/NeighborMetaData.h"
#include "../../Graph/Header/MyEdge.h"
//#include "../../../infrastructure/Configuration.h"


class NVChainStructure;
//class GraphMemoryManager;

// Is being initialized in NVRam
//
class NVRepository
{
protected:
    //
    const tvg_vertex_id m_lNodeID;

    constexpr static tvg_short_counter m_lInitialArraysSize = 10;

 //   std::map<tvg_long_counter, tvg_index> m_mapIncompleteModificationToIndex;

    bool m_bUnloaded;

    tvg_index m_lVertecesDoubleSize;
    tvg_index m_lVertecesCurrCompletedSize;
 //   tvg_index m_lVertecesReservedCurrSize;
    tvg_index m_lVertecesRemovedSize;


//    tvg_index m_lChainDoubleSize;
//    tvg_index m_lChainCurrSize;
//    tvg_index m_lChainRemovedSize;



    tvg_index m_lAdditionalChainDoubleLength;
    tvg_index m_lAdditionalChainCurrLength;
    tvg_index m_lAdditionalChainRemovedLength;

    tvg_index m_lSavedVertecesCurrCompletedSize;
    tvg_index m_lSavedVertecesRemovedSize;
//    tvg_index m_lSavedChainRemovedSize;
 //   tvg_index m_lSavedChainCurrSize;
    tvg_index m_lSavedAdditionalChainRemovedLength;
    tvg_index m_lSavedAdditionalChainCurrLength;


    log4cpp::Category& _log_root;



    // Data storage summary:
    //
    // 1. m_aNVAdjacencyList - is an array of data that can be immediatelly filled and is being filled by the left to right order. Each cell is of the equal size.
    // 2. m_aChainStructures - is an array of links, each of the same, application dependent predefined size. IT is being filled in an arbitrary order.
    // 3. m_aAdditionalChains - an array of links of the nodes, that isof a bigger size then one, defined for each cell of m_aChainStructures.


    // The NV data that is filled immediately after the node is allocated and can be immediately copied to the NRAM.
    //
    std::unique_ptr<AdjacentNVVertex[]> m_aNVAdjacencyList;


    // Chain of pointers (will be filled late and only then copied to the NVRAM).
    //
    std::unique_ptr<NVChainStructure[]> m_aChainStructures;



    // Chain of additional links
    //
    std::unique_ptr<SameNodeNVTemporalLink[]> m_aAdditionalChains;



    tvg_counter m_lSavedAdditionalHashSize;
    tvg_counter m_lAdditionalHashSize;
    std::unique_ptr<std::map<SameNodeNVLink, NVIntervalLink>> m_mapAdditionalNodesToChains;





    std::unique_ptr<SameNodeNVLink> m_linkEmpty;





private:
    //
    void DoubleVerticesAndChain();
//    void DoubleChain();
    void DoubleAdditionalChain();


    tvg_index BinarySearchPoint(const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast);
    tvg_index BinarySearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast, AdjacentNVVertex nvLastValue);


    tvg_index LoadNext(const tvg_index& lCurr, const tvg_time& tEndEpoch);
    tvg_index LoadNext_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch);

    void LoadMapAdditionalNodesToChains();
    void LoadMetadata();

public:
    //
    NVRepository(const tvg_vertex_id& lNodeID);

    ~NVRepository();

//    void ResetVerteces(tvg_index lInitialLength = Configuration::Instance()->GetIntegerItem("data-structure.initial-repository-size"));
//    void ResetChain(tvg_index lInitialLength = Configuration::Instance()->GetIntegerItem("data-structure.initial-repository-size"));
    void ResetVertecesAndChains(tvg_index lInitialLength = m_lInitialArraysSize);//Configuration::Instance()->GetIntegerItem("data-structure.initial-repository-size"));
    void ResetAdditionalChain(tvg_index lInitialLength = m_lInitialArraysSize); //Configuration::Instance()->GetIntegerItem("data-structure.initial-additional-chain-size"));
    void ResetMapAdditionalNodesToChains();


    bool AddChainStructure(std::unique_ptr<AdjacentDStructure>  pAdjacentDStructure, const tvg_index& indexOfComplete);

/*
    tvg_index Reserve(const tvg_long_counter& lUID, tvg_time tEpoch, tvg_vertex_id lNewNodeID, bool bSource);

    std::unique_ptr<AdjacentNVVertex> UpdateConnection_Optimal(const tvg_long_counter& lUID,
                                                       const tvg_vertex_id& lThisVertexID, const tvg_index& lThisVertexIndex,
                                                       std::unique_ptr<NVLink> pSecondLink,
                                                       std::unique_ptr<AdjacentDStructure> pCompleteRes,
                                                       const tvg_index& indexOfComplete);

    std::unique_ptr<AdjacentNVVertex> UpdateConnection_Advanced(const tvg_long_counter& lUID,
                                                               const tvg_vertex_id& lThisVertexID, const tvg_index& lThisVertexIndex,
                                                               std::unique_ptr<AdjacentDStructure> pCompleteRes,
                                                               const tvg_index& indexOfComplete);
*/


    std::unique_ptr<AdjacentNVVertex> ReserveAndUpdate(const tvg_long_counter& lUID, const tvg_vertex_id& lOtherVertexID,
                                                       std::unique_ptr<AdjacentDStructure> pCompleteRes, const tvg_index& indexOfComplete,
                                                       tvg_time tEpoch, bool bSource, MODIFICATION_TYPE m, tvg_counter& iSubmittedCount);


    void CompleteSetData(const tvg_vertex_id& lID, const tvg_index& lIndex, std::list<std::unique_ptr<SameNodeNVTemporalLink>>& lLinks, int iCounter, std::list<std::unique_ptr<SameNodeNVTemporalLink>>::iterator iterLinks);



    // Return a list of all the neighbors of the vertex. For each vertex keep the frequency counter (optional, 1 by default) and the timestamp of the last appearence.
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch);
    //std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_from_db(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch);
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_Efficient(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch);
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_Efficient_NVRAM(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch);




    tvg_index GetSuccessorInAdditionalLinks(const tvg_index& lCurr, const tvg_time& tEndEpoch);
    tvg_index GetSuccessorInAdditionalLinks_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch);



    void Evict();


    tvg_index GetCompletedVerticesNVLocation();
//    tvg_index GetCurrChainElementsCount();
    tvg_index GetCurrAdditionalChainElementsCount();
    tvg_index GetCurrMapAdditionalNodesToChainsElementsCount();
    tvg_index GetCurrMetadataSize();


    std::unique_ptr<AdjacentNVVertex[]> SubmitNVAdjacencyList();
    std::unique_ptr<NVChainStructure[]> SubmitChains();
    std::unique_ptr<SameNodeNVTemporalLink[]> SubmitAdditionalChains();
    std::unique_ptr<NVLinkPairInterval[]> SubmitMapAdditionalNodesToChains();
    std::unique_ptr<tvg_index[]> SubmitMetadata();

    std::unique_ptr<NVChainStructure[]>& GetChains();

    void ResetAll();






};



#endif //TVG_IGOR_NVREPOSITORY_H
