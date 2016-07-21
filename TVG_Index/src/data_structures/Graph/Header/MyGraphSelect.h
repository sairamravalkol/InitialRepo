//
// Created by norig on 12/23/15.
//

#ifndef TVG4TM_MYGRAPHSELECT_H
#define TVG4TM_MYGRAPHSELECT_H



#include "MyEdge.h"
#include "../../Parser/Header/Modification.h"
#include "../../General/Header/MyHistogram.h"
#include "MyVertex.h"
#include "GraphMemoryManager.h"
#include "../../../infrastructure/VertexProxy.h"
#include "DebugInfo.h"
#include "../../General/Header/tvg_vertex_id.h"

#include <unordered_set>



class AdditionsDeletionsLists
{
private:
    //
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> m_lAdditions;
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> m_lDeletions;


public:
    //
    AdditionsDeletionsLists(std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> lAdditions, std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> lDeletions)
    {
        m_lAdditions = std::move(lAdditions);
        m_lDeletions = std::move(lDeletions);
    }

    const std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>>& GetAdditionsList() const
    {
        return m_lAdditions;
    }

    const std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>>& GetDeletionsList() const
    {
        return m_lDeletions;
    }
};



class MyGraphSelect
{

////////////////////////////////////////////
    private:
    //
    log4cpp::Category& _log_root;
//    Vertica::ServerInterface& _log_root;

    std::unique_ptr<VertexProxy> m_vp;

    const std::string m_strGraphData;
    const bool m_bDirected;


    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>> m_mapIDNodes; // vertex id to vertex



    void LoadGraphInfo();


public:
    static std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> MergeLists(std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vOutput,
                                                                                   std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vCurrEdges,
                                                                                   tvg_time tShift);




    //
    MyGraphSelect(bool bDirected = true);
    ~MyGraphSelect();


    void SetSocId(int soc_id);

    std::unique_ptr<std::list<MyEdgeAdvanced>> KHop(const std::vector<VertexDepth>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth);
    std::unique_ptr<std::list<MyEdgeAdvanced>> KHop(const std::vector<VertexDepth>& vNodes, const std::map<tvg_vertex_id, int>& vExcludeNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth);

    std::unique_ptr<std::list<MyEdgeAdvanced>> LoadIntervalNeighbors(const std::vector<VertexDepth>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval);
    std::unique_ptr<std::list<MyEdgeAdvanced>> LoadIntervalNeighbors(const std::vector<VertexDepth>& vNodes, const std::map<tvg_vertex_id, int>& vExcludeNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval);



    std::unique_ptr<std::list<MyEdgeAdvanced>> KHopIntersection(const std::vector<VertexDepth>& vNodes1, const tvg_time& tStartInterval1, const tvg_time& tEndInterval1, int iMaxDepth1,
                                                                const std::vector<VertexDepth>& vNodes2, const tvg_time& tStartInterval2, const tvg_time& tEndInterval2, int iMaxDepth2);

    std::unique_ptr<std::list<tvg_time>> GetConnectionHistory(const MyEdge& e, const tvg_time& tStartInterval, const tvg_time& tEndInterval);

    std::unique_ptr<std::list<std::unique_ptr<std::list<MyEdgeAdvanced>>>> SnapConnection(const MyEdge& e, const tvg_time& tStartInterval, const tvg_time& tEndInterval, const tvg_time& tSnapWindowSize, int iMaxDepth = 3);
    std::unique_ptr<std::list<MyEdgeAdvanced>> EdgeListIntersection(std::unique_ptr<std::list<MyEdgeAdvanced>> listEdges1, std::unique_ptr<std::list<MyEdgeAdvanced>> listEdges2);

    std::unique_ptr<MyVertexSelect>&  GetVertex(tvg_vertex_id lID);


    std::unique_ptr<AdditionsDeletionsLists> GetAllNextConnections2(const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputNodes,
                                                                                  const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputExcludeNodes,
                                                                                  tvg_time tStartTime, tvg_time tEndTime,tvg_time tMaxWindowShift);

    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> GetAllNextConnections(const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputNodes,
                                                                             const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputExcludeNodes,
                                                                             tvg_time tStartTime, tvg_time tEndTime,tvg_time tMaxWindowShift);



    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> GetNextConnections(const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputNodes,
                                                                                           const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputExcludeNodes,
                                                                                           tvg_time tTime, tvg_time tMaxWindowShift, bool bAddition);


    bool ContainsVertex(tvg_vertex_id lID);



    tvg_vertex_id GetVertexCount()
    {
        return m_mapIDNodes.size();
    }

    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>>& GetVertices()
    {
        return m_mapIDNodes;
    }



    bool IsDirected()
    {
        return (m_bDirected == true);
    }


    bool AddVertex(const tvg_vertex_id& lID);



    bool IsModelEdge(tvg_vertex_id lCurrVertexID, std::unique_ptr<NeighborMetaData>& pNeighborMetaData, tvg_time tStartInterval, tvg_time tEndInterval);


};



#endif //TVG4TM_MYGRAPHSELECT_H
