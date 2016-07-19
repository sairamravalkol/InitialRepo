//
// Created by norig on 12/8/15.
//

#ifndef TVG4TM_MYVERTEXSELECT_H
#define TVG4TM_MYVERTEXSELECT_H

#include <log4cpp/Category.hh>
#include "PerformanceMeter.h"
#include "../../DRAM/Header/AdjacencyDList.h"
#include "../../NVRAM/Header/NVRepository.h"
//#include "../../../infrastructure/SendUtils.h"
//#include "../../../infrastructure/VertexProxy.h"
#include "../Header/MyVertexSelect.h"
//#include "../../../data_loaders/RawDataParser.h"


//#include "MyEdge.h"

class NeighborMetaData;
class MyEdgeTemporalAction;


class MyVertexSelect
{
private:
    //

    log4cpp::Category& _log_root;
//    Vertica::ServerInterface& _log_root;
    //
    //


    static tvg_index m_iOptimalAdjacentNVVertex;
    static tvg_index m_lSerialSearchMinSize;
    static tvg_short_counter m_lCurrMetadataSize;

    static bool m_bInterpolationSearch;

    tvg_index m_lVertecesCurrCompletedSize;
    tvg_index m_lVertecesRemovedSize;
    tvg_index m_lAdditionalChainRemovedLength;
    tvg_index m_lAdditionalChainCurrLength;

    tvg_index m_lAdditionalChainMatchingCurrLength;


    std::map<SameNodeNVLink, NVIntervalLink> m_mapAdditionalNodesToChains;
//    std::unique_ptr<NVTemporalLink[]> m_aAdditionalChains;


//	static const double m_dPerThreshold;

    const tvg_vertex_id m_lID;


    bool m_bLoaded;

    // private default constructor
    //
//    MyVertexSelect() : MyVertexSelect(-1) {};
    MyVertexSelect();

    void LoadMetadata();
    tvg_index LoadNext_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch);
    void LoadMapAdditionalNodesToChains();

    tvg_index SearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast);

    tvg_index BinarySearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast);
    tvg_index BinarySearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast, std::unique_ptr<AdjacentNVVertex[]> data, tvg_index lIndex);

    tvg_index InterpolationSearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast);
    tvg_index InterpolationSearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, std::unique_ptr<AdjacentNVVertex[]> nvFirstData, tvg_index lFirstDataIndex,
                                                                                               tvg_index lLast, std::unique_ptr<AdjacentNVVertex[]>  nvLastData, tvg_index lLastDataIndex);
    tvg_index SerialSearchPointLR_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast);
    tvg_index SerialSearchPointRL_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast);











public:
    //


    // SELECT
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_NVRAM(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch, const std::map<tvg_vertex_id, int>& vExcludeNodes);
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_NVRAM(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch);

//    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_NVRAM_Efficient(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch, const std::map<tvg_vertex_id, int>& vExcludeNodes, std::unique_ptr<SendUtils<> > sender);
//    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_NVRAM_Efficient(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch, std::unique_ptr<SendUtils<> > sender);


    //std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_from_db(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch,std::unique_ptr<SendUtils<> > sender) const;





    //
    std::unique_ptr<std::list<tvg_time>> GetLastConnectionAppearence(const tvg_vertex_id& lInputID, const tvg_time& tStartInterval, const tvg_time& tEndInterval);
    std::unique_ptr<std::list<tvg_time>> GetLastConnectionAppearence_Efficient(const tvg_vertex_id& lInputID, const tvg_time& tStartInterval, const tvg_time& tEndInterval);

    tvg_index GetSuccessorInAdditionalLinks_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch);



    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> GetNextConnections(const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputExcludeNodes,tvg_time tTime, tvg_time tMaxWindowShift, bool bAddition);


public:
    //
    MyVertexSelect(tvg_vertex_id lID);
    ~MyVertexSelect() {}





    const tvg_vertex_id GetID() const {return m_lID;}

//    const tvg_short_counter m_vOptimalBatchSize = 10000;

    tvg_index GetOptimalAdjacentNVVertex()
    {
        return m_iOptimalAdjacentNVVertex;
    }

    tvg_index GetSerialSearchMinSize()
    {
        return  m_lSerialSearchMinSize;
    }

    tvg_short_counter GetCurrMetadataSize()
    {
        return m_lCurrMetadataSize;
    }


    bool PerformInterpolationSearch()
    {
        return m_bInterpolationSearch;
    }


    static void SetOptimalAdjacentNVVertex(tvg_index iOptimalAdjacentNVVertex)
    {
        m_iOptimalAdjacentNVVertex = iOptimalAdjacentNVVertex;
    }

    static void SetSerialSearchMinSize(tvg_index lSerialSearchMinSize)
    {
        m_lSerialSearchMinSize = lSerialSearchMinSize;
    }

    static void SetCurrMetadataSize(tvg_short_counter lCurrMetadataSize)
    {
        m_lCurrMetadataSize = lCurrMetadataSize;
    }


    static void SetPerformInterpolationSearch(bool bInterpolationSearch)
    {
        m_bInterpolationSearch = bInterpolationSearch;
    }

};

#endif //TVG4TM_MYVERTEXTHIN_H
