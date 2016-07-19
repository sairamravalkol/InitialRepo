//
// Created by norig on 6/21/15.
//

#ifndef TVG_IGOR_MYGRAPH_H
#define TVG_IGOR_MYGRAPH_H


#include <stdio.h>
#include <map>
#include <log4cpp/Category.hh>
//#include "../Header/MyVertex.h"
#include "MyEdge.h"
#include "../../Parser/Header/Modification.h"
#include "../../General/Header/MyHistogram.h"
#include "MyVertex.h"
#include "GraphMemoryManager.h"
#include "../../../infrastructure/VertexProxy.h"
#include "DebugInfo.h"


enum EFFICIENCY_LEVEL {LEVEL_SIMPLE, LEVEL_ADVANCED, LEVEL_OPTIMAL};


class MyGraph
{

////////////////////////////////////////////
private:
    //

//    Vertica::ServerInterface& _log_root;
    log4cpp::Category& _log_root;
    tvg_vertex_id _report;
    std::unique_ptr<VertexProxy> m_vp;

    bool m_bCompleteChain; // for optimization


    const EFFICIENCY_LEVEL m_eEfficiencyLevel;


    const tvg_long_counter m_iDebugMaxModificationCount;





protected:
    //
    const bool m_bDirected;

    tvg_long_counter m_lTotalConnectionsCount;

//    static final int INCOMING = 0;
//    static final int OUTGOING = 1;
//    static final int INCIDENT = 2;

    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>> m_mapIDNodes; // vertex id to vertex


    //   std::map<long, AdjacencyListTemporal*, VertexCompare> m_mapNodesToAdList; // Map of vertices to adjacency maps of vertices to {incoming, outgoing, incident} edges


//    std::map<VertexIDs*, E*, EdgeCompare> m_vDirectedEdges;    // Map of edges by the ID of it vertices. Mostly used for a fast search of an MyEdge between two points.


    //   std::map<long, E*>* m_vUndirectedEdges;    //


    std::unique_ptr<GraphMemoryManager> m_pGraphMemoryManager;


    std::unique_ptr<MyHistogram> m_pHistChain;





public:
    //
    MyGraph(bool bDirected = true);
    ~MyGraph();


    void SetSocId(int soc_id);

//    std::unique_ptr<std::list<MyEdgeAdvanced>> BFS_Depth(const std::vector<tvg_vertex_id>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth);
    std::unique_ptr<std::list<MyEdgeAdvanced>> BFS_Depth_Node_Efficient(const std::vector<VertexDepth>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth);
//    std::unique_ptr<std::list<MyEdgeAdvanced>> BFS_Depth_Node_Efficient_NVRAM(const std::vector<tvg_vertex_id>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth);




    // returns the size of released memory
    //
    /*
    unsigned long ReleaseMemory()
    {

    }
*/

    // returns the size of released memory
    //


    void FinalizeAndSubmitNodes();


    void ReleaseGraph();


    /*
    void PrintAll()
    {
        m_debugInfo.PrepareStorage();

        tvg_counter lReleaseSize = 0;
        std::map<tvg_vertex_id, std::unique_ptr<VertexMemoryData>>::iterator iter = m_mapIDVertexMemory.begin();
        for ( ; iter !=  m_mapIDNodes.end() ; iter++)
        {
            m_debugInfo->Print(lID, iter->second->SubmitNVAdjacencyList(), iterNode->second->GetCompletedVerticesElementsCount());
            m_debugInfo->Print(lID, iterNode->second->SubmitChains(), iterNode->second->GetCurrChainElementsCount());

            m_debugInfo->Print(lID, iterNode->second->SubmitAdditionalChains(), iterNode->second->GetCurrAdditionalChainElementsCount());
            m_debugInfo->Print(lID, iterNode->second->SubmitMapAdditionalNodesToChains(), iterNode->second->GetCurrMapAdditionalNodesToChainsElementsCount());
        }
    }
*/



    //////////////////////////////



    void Finalize();


    /*
    E* FindEdge(long l1, long l2)
    {
        V* vSource = GetVertex(l1);
        if (vSource == nullptr)
            return nullptr;

        V* vSource = GetVertex(l1);
        if (vSource == nullptr)
            return nullptr;


        m_pTempVertexID->SetValue(l1, l2);
        std::map<VertexIDs*, E*, EdgeCompare>::iterator iter = m_vDirectedEdges.find(m_pTempVertexID);
        if (iter == m_vDirectedEdges.end())
            return nullptr;
        else
            return iter.second;
    }


    E* findEdge(V* pV1, V* pV2)
    {
        if ((pV1 == nullptr) || (pV2 == nullptr))
            return nullptr;

        return FindEdge(pV1->GetID(), pV2->GetID());
    }
    */


    /*
    public Collection<E> findEdgeSet(V v1, V v2)
    {
        if (!containsVertex(v1) || !containsVertex(v2))
            return nullptr;
        Collection<E> edges = new ArrayList<E>(2);
        E e1 = vertex_maps.get(v1)[OUTGOING].get(v2);
        if (e1 != nullptr)
            edges.add(e1);
        E e2 = vertex_maps.get(v1)[INCIDENT].get(v2);
        if (e1 != nullptr)
            edges.add(e2);
        return edges;
    }
    */


    std::unique_ptr<MyVertex>&  GetVertex(tvg_vertex_id lID);


    bool ContainsVertex(tvg_vertex_id lID);


 //   void UpdateConnection(const tvg_long_counter& lUID, tvg_vertex_id lVertexID, tvg_time lEpoch, std::unique_ptr<NVLink> pSecondLink);



    /*
    bool ContainsEdge(VertexIDs* pVertexID)
    {
        std::map<VertexIDs*, E*, EdgeCompare>::iterator iter = m_vDirectedEdges.find(pVertexID);
        if (iter == m_vDirectedEdges.end())
            return false;
        else
            return true;
    }


    bool ContainsEdge(E* pEdge)
    {
        m_pTempVertexID->SetValue(pEdge->GetVertexBaseIn()->GetID(), pEdge->GetVertexBaseOut()->GetID());
        return ContainsEdge(m_pTempVertexID);
    }


    int GetEdgeCount()
    {
        return m_vDirectedEdges.size();
    }
    */

    tvg_vertex_id GetVertexCount()
    {
        return m_mapIDNodes.size();
    }

    tvg_long_counter GetConnectionCount() const
    {
        return m_lTotalConnectionsCount;
    }


    bool IsDirected()
    {
        return (m_bDirected == true);
    }

    /*
    AdjacencyListTemporal* GetNeighbors(MyVertex* pVertex)
    {
        return pVertex->GetAdjacencyList();
    }
*/

//    bool AddVertex(MyVertex* pVertex);

    bool AddVertex(tvg_vertex_id lID, tvg_time tEpoch);

    bool AddUndirectedModification(std::unique_ptr<Modification> pModification);
    bool AddDirectedModification(std::unique_ptr<Modification> pModification);

    bool EvictMemory();

    bool IsMemoryHeapCorrect();

    tvg_long_counter GetRealIndexToDSStructureSize();
    tvg_long_counter GetRealSameValueToLastLocationSize();


private:
    //
    bool AddDirectedModification_Advanced(std::unique_ptr<Modification> pModification);
 //   bool AddDirectedModification_Optimal(std::unique_ptr<Modification> pModification);





    /*
     // returns false if a new MyEdge was not added
    //
    bool AddEdge(E* pEdge)
    {
        V* pV1 = MyEdge->GetVertexBaseIn();
        V* pV2 = MyEdge->GetVertexBaseOut();

        E* edgeTemp = FindEdge(pV1, pV2);
        if (edgeTemp != nullptr)
            return false;

        if (!ContainsVertex(pV1))
            AddVertex(pV1);

        if (!ContainsVertex(pV2))
            AddVertex(pV2);


        // map v1 to <v2, MyEdge> and vice versa
        //
        if (m_bDirected)
        {
            VertexIDs* newVertexIDs = new VertexIDs(pV1->GetID(), pV2->GetID());
            m_vDirectedEdges[newVertexIDs] = pEdge;
        }
        else
        {
             VertexIDs* newVertexIDs1 = new VertexIDs(pV1->GetID(), pV2->GetID());
            m_vDirectedEdges[newVertexIDs1] = pEdge;

            VertexIDs* newVertexIDs2 = new VertexIDs(pV2->GetID(), pV1->GetID());
            m_vDirectedEdges[newVertexIDs2] = pEdge;
        }

        return true;
    }
    */




    /*


                          std::map<long, V*> m_mapIDNodes;

                          std::map<VertexIDs*, E*, EdgeCompare> m_vDirectedEdges;    // Map of ed

                          */




/*
    bool RemoveVertex(V* pVertex)
    {
        if (!ContainsVertex(pVertex))
            return false;


        // copy to avoid concurrent modification in removeEdge
        Collection<E> incident = new ArrayList<E>(getIncidentEdges(vertex));

        for (E MyEdge : incident)
            removeEdge(MyEdge);

        vertex_maps.remove(vertex);

        return true;
    }
*/

    /*
    public boolean removeEdge(E* pEdge)
    {
        if (!ContainsEdge(MyEdge))
            return false;

        Pair<V> endpoints = getEndpoints(MyEdge);
        V v1 = endpoints.getFirst();
        V v2 = endpoints.getSecond();

        // remove MyEdge from incident vertices' adjacency maps
        if (getEdgeType(MyEdge) == EdgeType.DIRECTED)
        {
            vertex_maps.get(v1)[OUTGOING].remove(v2);
            vertex_maps.get(v2)[INCOMING].remove(v1);
            directed_edges.remove(MyEdge);
        }
        else
        {
            vertex_maps.get(v1)[INCIDENT].remove(v2);
            vertex_maps.get(v2)[INCIDENT].remove(v1);
            undirected_edges.remove(MyEdge);
        }

        return true;
    }

    int getEdgeCount(EdgeType edge_type)
    {
        if (edge_type == EdgeType.DIRECTED)
            return directed_edges.size();
        if (edge_type == EdgeType.UNDIRECTED)
            return undirected_edges.size();
        return 0;
    }
    */

};



#endif //TVG_IGOR_MYGRAPH_H
