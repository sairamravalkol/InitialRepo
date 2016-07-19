//
// Created by norig on 6/21/15.
//


#include <queue>


#include "../Header/MyGraph.h"
//#include "../../../infrastructure/SendUtils.h"
#include "../../../infrastructure/VertexProxy.h"
//#include "../../../infrastructure/Uid.h"






/////////////////// Construction
//

//MyGraph::MyGraph(Vertica::ServerInterface &srvInterface,bool bDirected) : _log_root(log4cpp::Category::getRoot()), m_bDirected(bDirected),_report(1000), m_iDebugMaxModificationCount(1), m_eEfficiencyLevel(LEVEL_ADVANCED)
MyGraph::MyGraph(bool bDirected) : _log_root(log4cpp::Category::getRoot()), m_bDirected(bDirected),_report(1000), m_iDebugMaxModificationCount(1), m_eEfficiencyLevel(LEVEL_ADVANCED)

{
    _log_root.debug("MyGraph::MyGraph");


    if (m_bDirected)
    {
//			m_vDirectedEdges = new std::map<long, E*>();
//			m_vUndirectedEdges = nullptr;    //
    }

    else
    {
//			m_vDirectedEdges = nullptr;
//			m_vUndirectedEdges = new std::map<long, E*>();
    }



    m_lTotalConnectionsCount = 0;
    m_bCompleteChain = false;
 //   m_pGraphMemoryManager = new GraphMemoryManager(m_mapIDNodes, _log_root);

    m_pGraphMemoryManager = std::make_unique<GraphMemoryManager>(m_mapIDNodes);
    m_vp = std::make_unique<VertexProxy>(-1);//define an invalid soc - need to call SetSoc when known

}



MyGraph::~MyGraph()
{

    _log_root.info("MyGraph::~MyGraph Total number of vertices: " + std::to_string(m_lTotalConnectionsCount)+" in soc "+std::to_string(m_vp->GetSocId()));




/*
		for (std::map<V*, AdjacencyListTemporal*>::iterator iter = m_mapNodesToAdList.begin() ;  iter != m_mapNodesToAdList.end() ; iter++)
		{
			if (iter->first != nullptr)
				delete iter->first;

			if (iter->second != nullptr)
				delete iter->second;

			m_mapNodesToAdList.clear();
		}
 */

    /*
    std::map<long, E*>::iterator iter = m_vDirectedEdges.begin() ;  iter != m_vDirectedEdges.end() ; iter++)
    {
        if (iter->second != nullptr)
                delete iter->second;

        m_vDirectedEdges.clear();
    }

    */

/*		std::map<long, E*>::iterator iter = m_vUndirectedEdges.begin() ;  iter != m_vUndirectedEdges.end() ; iter++)
		{
			if (iter->second != nullptr)
					delete iter->second;
		}
*/

//		if (m_vDirectedEdges)
//			delete m_vDirectedEdges;

//		if (m_vUndirectedEdges)
//			delete m_vUndirectedEdges;

}


void MyGraph::SetSocId(int soc_id)
{
    m_vp = std::make_unique<VertexProxy>(soc_id);
}


bool MyGraph::AddUndirectedModification(std::unique_ptr<Modification> pModification)
{
    if (pModification->GetSource() != pModification->GetDestination())     // for case of self-edge
    {
        std::unique_ptr<Modification> pReverseModification = std::make_unique<Modification>(pModification->GetUID(), pModification->GetEpoch(), pModification->GetModificationType(),
                                                              pModification->GetDestination(), pModification->GetSource(), !pModification->GetIsReversed());

        AddDirectedModification(std::move(pModification)); // to keep the order
        AddDirectedModification(std::move(pReverseModification));
    }
    else
    {
        AddDirectedModification(std::move(pModification));
    }
}

bool MyGraph::IsMemoryHeapCorrect()
{
    return m_pGraphMemoryManager->IsMemoryHeapCorrect();
}




bool MyGraph::AddDirectedModification(std::unique_ptr<Modification> pModification)
{

    _log_root.debug("MyGraph::AddDirectedModification");
    switch (m_eEfficiencyLevel)
    {
        case LEVEL_ADVANCED:
        {
            return AddDirectedModification_Advanced(std::move(pModification));
        }

/*
        case LEVEL_OPTIMAL:
        {
            return AddDirectedModification_Optimal(std::move(pModification));
        }
*/

        default:
        {
            _log_root.debug("AddDirectedModification: efficiency level is SIMPLE");
            return false;
        }

    }
}



// AddModification(A->B + metadata) adds b to the list of a.
// It conssists of 2 steps: Reservation and Update.
// The output of Reservation of B in A is requeired while Updating B.
// Therefore, for an addition of A->B edge we perform 4 operations:
// a, Reserve(A, B)
// b. Reserve(B, A)
// c. Update(A, B)
// d. Update(B, A).

// a is always called before c, and b before d, but other orders are possible.

// In the multi-threading, we have the following possible oreds:
// 1. a->b->c->d (a->b->d->c, b->a->c->d, b->a->d->c). No synchronization problem.
// 2. a->c->b->
/*
bool MyGraph::AddDirectedModification_Optimal(std::unique_ptr<Modification> pModification) //////////////////////////////// need to improve the cross calls
{
    _log_root.debug("MyGraph::AddModification Start");

    // Create verteces, if not exist
    //
    if (!ContainsVertex(pModification->GetSource()))
    {
        if(!AddVertex(pModification->GetSource(), pModification->GetEpoch()))
            return false;
    }
    std::unique_ptr<MyVertex>& pV1 = GetVertex(pModification->GetSource());


    if (m_vp->IsVertexLocal(pModification->GetDestination()))
    {
        if (!ContainsVertex(pModification->GetDestination()))
        {
//            _log_root.error("MyGraph::AddModification : local destination vertex " + std::to_string(pModification->GetDestination()) + " does not exists.");

            if(!AddVertex(pModification->GetDestination(), pModification->GetEpoch()))
            {
                _log_root.error("MyGraph::AddModification : could not add local vertex " + std::to_string(pModification->GetDestination()));
                return false;
            }
        }
        std::unique_ptr<MyVertex>& pV2 = GetVertex(pModification->GetDestination());


        tvg_counter iFirstCompleteCounter;
        tvg_counter iSecondCompleteCounter;

        m_bCompleteChain = pV1->AddLocalConnection(pV2, pModification->GetUID(), pModification->GetEpoch(), !pModification->GetIsReversed(), iFirstCompleteCounter, iSecondCompleteCounter);

        // use the output of AddLocalConnection
        //
        m_pGraphMemoryManager->UpdateNode(pV1->GetID(), 1, (iFirstCompleteCounter > 0), iFirstCompleteCounter);
        if (iSecondCompleteCounter > 0)
            m_pGraphMemoryManager->UpdateNode(pV2->GetID(), 0, (iSecondCompleteCounter > 0), iSecondCompleteCounter);


    } // std::unique_ptr<VertexProxy> m_vp;
    else
    {
        tvg_counter iFirstCompleteCounter;
        pV1->AddDistantConnection(m_vp, pModification->GetDestination(), pModification->GetUID(), pModification->GetEpoch(), pModification->GetDestination(), !pModification->GetIsReversed(), iFirstCompleteCounter);
        m_pGraphMemoryManager->UpdateNode(pV1->GetID(), 1, (iFirstCompleteCounter > 0), iFirstCompleteCounter);

    }



    // Create links to the future nodes  const tvg_long_counter& lUID, tvg_time tEpoch, tvg_vertex_id lNewNodeID, bool bSource
    //



        // Add connections
        //
//        pV1->UpdateConnection(pV1->GetID(), pFirstLink, pSecondLink, pModification->GetUID(), pModification->GetEpoch(), m_bCompleteChain);
//        m_pGraphMemoryManager->UpdateNode(pV1->GetID(), 1, m_bCompleteChain);

//        pV2->UpdateConnection(pV1->GetID(), pSecondLink, pFirstLink, pModification->GetUID(), pModification->GetEpoch(), m_bCompleteChain);




    m_lTotalConnectionsCount++;



    if(GetVertexCount() > _report)
    {
        _log_root.info("insert " + std::to_string(GetVertexCount()) + " vertices to graph");
        _report += tvg_vertex_id(1000);
    }
    return true;
}

*/







// Add a modification
//
bool MyGraph::AddDirectedModification_Advanced(std::unique_ptr<Modification> pModification)
{
    /*
    tvg_vertex_id lSource = pModification->GetSource();
    tvg_vertex_id lDestination = pModification->GetDestination();

    if (lSource == lDestination)
    {
        if (lSource == tvg_vertex_id(159))
        {
            int t = 1;
        }
    }
*/


    // Create verteces, if does not exist
    //
    if (!ContainsVertex(pModification->GetSource()))
    {
        if(!AddVertex(pModification->GetSource(), pModification->GetEpoch()))
            return false;
    }
    _log_root.debug("AddDirectedModification_Advanced");
    std::unique_ptr<MyVertex>& pV1 = GetVertex(pModification->GetSource());





    tvg_counter iChainCounter = pV1->AddConnection_Advanced(pModification->GetUID(), pModification->GetDestination(), pModification->GetEpoch(), !pModification->GetIsReversed(), pModification->GetModificationType());



    // Update the memory state, based on the connection addition
    //
    tvg_counter iAdditionalChainCounter = (iChainCounter > NVChainStructure::GetMaxChainSize()) ? (iChainCounter - NVChainStructure::GetMaxChainSize()) : 0;
    m_pGraphMemoryManager->UpdateNode(pV1->GetID(), 1, (iChainCounter > 0), iAdditionalChainCounter);


    m_lTotalConnectionsCount++;

/*
    if ((m_lTotalConnectionsCount % 1000) == 0)
    {
        std::cout<<"DSStructureSize = "<<GetRealIndexToDSStructureSize()<<std::endl;
        std::cout<<"SameValueToLastLocationSize = "<<GetRealSameValueToLastLocationSize()<<std::endl;
    }
*/


    if(GetVertexCount() > _report)
    {
        _log_root.debug("insert %llu vertices to graph",(GetVertexCount().Get()));
        _report += tvg_vertex_id(1000);
    }
    return true;
}


tvg_long_counter MyGraph::GetRealIndexToDSStructureSize()
{
    tvg_long_counter lRes = 0;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iter = m_mapIDNodes.begin();
    for ( ; iter != m_mapIDNodes.end() ; iter++)
    {
        lRes += iter->second->GetRealIndexToDSStructureSize();
    }

    return lRes;
}

tvg_long_counter MyGraph::GetRealSameValueToLastLocationSize()
{
    tvg_long_counter lRes = 0;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iter = m_mapIDNodes.begin();
    for ( ; iter != m_mapIDNodes.end() ; iter++)
    {
        lRes += iter->second->GetRealSameValueToLastLocationSize();
    }

    return lRes;
}


/*
void MyGraph::UpdateConnection(const tvg_long_counter& lUID, tvg_vertex_id lVertexID, tvg_time lEpoch, std::unique_ptr<NVLink> pSecondLink)
{
    if (m_vp->IsVertexLocal(lVertexID))
    {
        if (!ContainsVertex(lVertexID))
        {
//            _log_root.error("MyGraph::AddModification : local destination vertex " + std::to_string(pModification->GetDestination()) + " does not exists.");

            if (!AddVertex(lVertexID, lEpoch))
            {
                _log_root.error("MyGraph::UpdateConnection: could not add local vertex " +
                                std::to_string(lVertexID));
                return;
            }
        }
        std::unique_ptr<MyVertex> &pV2 = GetVertex(lVertexID);

        tvg_counter iAdditionalCompleteCounter;
        pV2->UpdateConnection(lUID, std::move(pSecondLink), iAdditionalCompleteCounter);
        m_pGraphMemoryManager->UpdateNode(pV2->GetID(), 1, (iAdditionalCompleteCounter > 0), iAdditionalCompleteCounter);
    }
    else
    {
        auto soc_is = m_vp->GetSocId();
        _log_root.error("MyGraph::UpdateConnection["+std::to_string(soc_is)+"]: the vertex " +
                        std::to_string(lVertexID) + " is not local");
    }


}
*/


/*
bool MyGraph::AddModification(std::unique_ptr<Modification> pModification)
{
    _log_root.debug("MyGraph::AddModification Start");


    // Create verteces, if not exist
    //
    if (!ContainsVertex(pModification->GetSource()))
    {
        if(!AddVertex(pModification->GetSource(), pModification->GetEpoch()))
            return false;
    }
    std::unique_ptr<MyVertex>& pV1 = GetVertex(pModification->GetSource());




    if (!ContainsVertex(pModification->GetDestination()))
    {
        if(!AddVertex(pModification->GetDestination(), pModification->GetEpoch()))
            return false;
    }
    std::unique_ptr<MyVertex>& pV2 = GetVertex(pModification->GetDestination());




//            _log_root.error("inserting " + std::to_string(m_lTotalConnectionsCount + 1) + " modifications: (" + std::to_string(pModification->GetSourceVertex()[i]) + ", " + std::to_string(pModification->GetDestinationVerteces()[j]) + "), " + std::to_string(pModification->GetEpoch()));



    // Create links to the future nodes  const tvg_long_counter& lUID, tvg_time tEpoch, tvg_vertex_id lNewNodeID, bool bSource
    //
    std::unique_ptr<NVLink> pFirstLink = pV1->Reserve(pModification->GetUID(), pModification->GetEpoch(), pV2->GetID(), true);
    if (pV1 == pV2)     // for case of self-edge
    {
        pV1->UpdateConnection(pV2->GetID(), pFirstLink, pFirstLink, pModification->GetUID(), pModification->GetEpoch(), m_bCompleteChain);
        m_pGraphMemoryManager->UpdateNode(pV1->GetID(), 1, m_bCompleteChain);
    }
    else
    {
        std::unique_ptr<NVLink> pSecondLink = pV2->Reserve(pModification->GetUID(), pModification->GetEpoch(), pV1->GetID(), !m_bDirected);



        // Add connections
        //
        pV1->UpdateConnection(pV2->GetID(), pFirstLink, pSecondLink, pModification->GetUID(), pModification->GetEpoch(), m_bCompleteChain);
        m_pGraphMemoryManager->UpdateNode(pV1->GetID(), 1, m_bCompleteChain);


        pV2->UpdateConnection(pV1->GetID(), pSecondLink, pFirstLink, pModification->GetUID(), pModification->GetEpoch(), m_bCompleteChain);
        m_pGraphMemoryManager->UpdateNode(pV2->GetID(), 1, m_bCompleteChain);
    }




    m_lTotalConnectionsCount++;



//    if(GetVertexCount() > _report)
//    {
//        _log_root.info("insert " + std::to_string(GetVertexCount()) + " vertices to graph");
//        _report += tvg_vertex_id(1000);
//    }
    return true;
}
*/



bool MyGraph::AddVertex(tvg_vertex_id lID, tvg_time tEpoch)
{
    _log_root.debug("MyGraph::AddVertex Start");

    if (lID < tvg_vertex_id(0))//SAGI - remove the ref to Global with direct compute. This is not a configuration item. Probably need to be a function !!!
    {
        _log_root.debug("invalid liD  %llu",lID.Get());
        return false;
    }

    if (!ContainsVertex(lID))
    {
        std::unique_ptr<MyVertex> pNewVertex = std::make_unique<MyVertex>(lID, tEpoch);
        m_mapIDNodes[lID] = std::move(pNewVertex);
    }
    else
    {
        //std::cout<<"FUCKKKKK"; // exception, should not happen
        _log_root.error("MyGraph::AddVertex: Addidng existing vertex %llu" ,(lID.Get()));
        return false;
    }

    return true;
}


std::unique_ptr<MyVertex>& MyGraph::GetVertex(tvg_vertex_id lID)
{
//    _log_root.debug("MyGraph::GetVertex Start");

    if (!ContainsVertex(lID))
    {
        _log_root.debug("MyGraph::GetVertex Getting non existant vertex - contains query expected");
    }

    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iter = m_mapIDNodes.find(lID);
    return iter->second;
}


bool MyGraph::ContainsVertex(tvg_vertex_id lID)
{
    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iter = m_mapIDNodes.find(lID);
    return (iter != m_mapIDNodes.end());
}






void MyGraph::FinalizeAndSubmitNodes()
{
    // Finalize
    //
    tvg_index iChainCounter = 0;
    tvg_index iAdditionalChainCounter;

    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iter = m_mapIDNodes.begin();

    int index=0;
    for ( ; iter != m_mapIDNodes.end() ; iter++)
    {
        iAdditionalChainCounter = iter->second->Finalize(iChainCounter);
        m_pGraphMemoryManager->UpdateNode(iter->second->GetID(), 0, iChainCounter, iAdditionalChainCounter);
        index++;
    }



    // Submit
    //
    m_pGraphMemoryManager->SubmitAll();
    _log_root.debug("MyGraph::FinalizeAndSubmitNodes: finish");




}



void MyGraph::Finalize()
{
    /*
    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iter = m_mapIDNodes.begin();
    for ( ; iter != m_mapIDNodes.end() ; iter++)
        iter->second->Finalize();
        */

    FinalizeAndSubmitNodes();


    ReleaseGraph();
}


void MyGraph::ReleaseGraph()
{

}


















//////////////////// End Construction
















//////////////////// Begin Search


/*

std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraph::BFS_Depth(const std::vector<tvg_vertex_id>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth)
{
    std::unique_ptr<std::list<MyEdgeAdvanced>> pMapResult = std::make_unique<std::list<MyEdgeAdvanced>>();

    if((iMaxDepth < 0) || (vNodes.empty()))
    {
        return pMapResult;
    }



    // The queue of vertices
    //
    std::priority_queue<VertexDepth, std::vector<VertexDepth>, CompareVertexDepth> qVertices;


    // A hash (and an iterator) of marked nodes
    //
    std::map<tvg_vertex_id, int> hashIDToDepth;
    std::map<tvg_vertex_id, int>::iterator iterMarkedNodes;



    // Initialize
    //
    //fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
    for (int i = 0 ; i < vNodes.size() ; i++)
    {
        VertexDepth newVertexDepth(vNodes[i], 0l);
      //  fs<<("Add to priority queue: Vertex " + std::to_string(newVertexDepth.GetVertexID())+ ", Depth " + std::to_string(newVertexDepth.GetDepth()))<<std::endl;
        qVertices.push(newVertexDepth);
        hashIDToDepth[vNodes[i]] = 0;
    }
    //fs.close();




    std::vector<std::unique_ptr<NeighborMetaData>>::iterator iterCurrNodeResults;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iterNewNode;




    // Main loop
    //
    for ( ; !qVertices.empty() ; qVertices.pop())
    {
        // shorted depth
        //
        const VertexDepth current(qVertices.top());
        int lCurrDepth = current.GetDepth();



      //  fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
      //  fs<<("Top priority queue: Vertex " + std::to_string(current.GetVertexID())+ ", Depth " + std::to_string(current.GetDepth()))<<std::endl;
      //  fs.close();



        if (lCurrDepth >= iMaxDepth)
             break;


        // if the same node was already inserted with a smaller depth - do not develop the node
        //
        iterMarkedNodes = hashIDToDepth.find(current.GetVertexID());
        if (iterMarkedNodes != hashIDToDepth.end())
        {
            if (iterMarkedNodes->second < current.GetDepth())
                continue;
        }


        // Locate the node and bring its neighbors
        //
        iterNewNode = m_mapIDNodes.find(current.GetVertexID());
        if (iterNewNode == m_mapIDNodes.end())
        {
            _log_root.error("MyGraph::BFS_Depth Could not find node "+ std::to_string(current.GetVertexID()));
            continue;
        }

        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> pCurrNeighbors = iterNewNode->second->LoadIntervalNeighbors(tStartInterval, tEndInterval,std::unique_ptr <SendUtils<> >());
        //fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
        //fs<<("Node " + std::to_string(current.GetDepth())+ ", Neighbors size " + std::to_string(pCurrNeighbors->size()))<<std::endl;
        //fs.close();


        // Add results to the queue
        //
//        for (iterCurrNodeResults = pCurrNeighbors->begin() ; iterCurrNodeResults != pCurrNeighbors->end() ; iterCurrNodeResults++)
        for(int i = 0 ; i < pCurrNeighbors->size() ; i++)
        {
            std::unique_ptr<NeighborMetaData> pNeighborMetaData = std::move(((*pCurrNeighbors)[i]));
            // add to the result
            //
            MyEdgeAdvanced newEdge(current.GetVertexID(), pNeighborMetaData->GetID(), pNeighborMetaData->GetEpoch(), lCurrDepth + 1, (i == pCurrNeighbors->size() - 1), pNeighborMetaData->GetCounter());// long lSource, long lDestination, tvg_time tEpoch, long lFrequency = 1
            pMapResult->push_back(newEdge);


            iterMarkedNodes = hashIDToDepth.find(pNeighborMetaData->GetID());
            if (iterMarkedNodes == hashIDToDepth.end()) // i.e. first visit
            {
                VertexDepth newDepth(pNeighborMetaData->GetID(), lCurrDepth + 1);

          //      fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
           //     fs<<("Add to priority queue: Vertex " + std::to_string(newDepth.GetVertexID())+ ", Depth " + std::to_string(newDepth.GetDepth()))<<std::endl;
             //   fs.close();

                qVertices.push(newDepth);
                hashIDToDepth[pNeighborMetaData->GetID()] = lCurrDepth + 1;

            }
            else
            {
                if (iterMarkedNodes->second <= lCurrDepth + 1)
                {
               //     fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
               //     fs<<("Skipped: Vertex " + std::to_string(pNeighborMetaData->GetID())+ ", Depth " + std::to_string(lCurrDepth + 1))<<std::endl;
               //     fs.close();

                    continue;
                }
                else // if the new node has a smaller depth - add it with the same value a a smaller key
                {
                    VertexDepth newDepth(pNeighborMetaData->GetID(), lCurrDepth + 1);

                 //   fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
                  //  fs<<("Add to priority queue: Vertex " + std::to_string(newDepth.GetVertexID())+ ", Depth " + std::to_string(newDepth.GetDepth()))<<std::endl;
                   // fs.close();

                    qVertices.push(newDepth);
                    hashIDToDepth[pNeighborMetaData->GetID()] = lCurrDepth + 1;
                }
            }


        }
    }


    return std::move(pMapResult);
}

*/



std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraph::BFS_Depth_Node_Efficient(const std::vector<VertexDepth>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth)
{
    //_log_root.debug("MyGraph::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] Start");
    std::unique_ptr<std::list<MyEdgeAdvanced>> pMapResult = std::make_unique<std::list<MyEdgeAdvanced>>();

    if((iMaxDepth < 0) || (vNodes.empty()))
    {
        return pMapResult;
    }

    //std::fstream fs("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out);
    //fs<<("Begin BFS. Start time: " + tStartInterval.GetString() + ", End time " + tEndInterval.GetString() + ", Max Depth " + std::to_string(iMaxDepth) )<<std::endl;
    //fs.close();


    // The queue of vertices
    //
    std::priority_queue<VertexDepth, std::vector<VertexDepth>, CompareVertexDepth> qVertices;


    // A hash (and an iterator) of marked nodes
    //
    std::map<tvg_counter, int> hashIDToDepth;
    std::map<tvg_counter, int>::iterator iterMarkedNodes;



    // Initialize
    //
    for (int i = 0 ; i < vNodes.size() ; i++)
    {
        VertexDepth newVertexDepth(vNodes[i].GetVertexID(), vNodes[i].GetDepth());
  //      fs<<("Add to priority queue: Vertex " + std::to_string(newVertexDepth.GetVertexID())+ ", Depth " + std::to_string(newVertexDepth.GetDepth()))<<std::endl;
        qVertices.push(newVertexDepth);
        hashIDToDepth[vNodes[i].GetVertexID()] = vNodes[i].GetDepth();
    }
    //fs.close();




    std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterCurrNodeResults;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iterNewNode;


    // Main loop
    // we will want to make this loop multi threaded
    //
    //#pragma omp parallel
    //#pragma omp single nowait
    //{
    //then we need to add the task interface and the lock on the queue of vertices
    for ( ; !qVertices.empty() ; qVertices.pop())
    {

        // shorted depth
        //
        const VertexDepth current(qVertices.top());
        int lCurrDepth = current.GetDepth();

        //_log_root.warn("working on vertex "+ std::to_string(current.GetVertexID()));

        //fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
        //fs<<("Top priority queue: Vertex " + std::to_string(current.GetVertexID())+ ", Depth " + std::to_string(current.GetDepth()))<<std::endl;
        //fs.close();

        if (!m_vp->IsVertexLocal(current.GetVertexID()))
        {
            //eyal
//            _log_root.log("MyGraphSelect::KHop["+std::to_string(m_vp->GetSocId())+"]: the vertex " +
//                           std::to_string(current.GetVertexID()) + " is not local");

            continue;
        }


        if (lCurrDepth >= iMaxDepth)
            break;


        // if the same node was already inserted with a smaller depth - do not develop the node
        //
        iterMarkedNodes = hashIDToDepth.find(current.GetVertexID());
        if (iterMarkedNodes != hashIDToDepth.end())
        {
            if (iterMarkedNodes->second < current.GetDepth())
                continue;
        }


        // Locate the node and bring its neighbors
        //
        iterNewNode = m_mapIDNodes.find(current.GetVertexID());
        if (iterNewNode == m_mapIDNodes.end())
        {
            //eyal
            //demote this to info since it is valid in the case of distributed graph
//            _log_root.info("MyGraph::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] Could not find node "+ std::to_string(current.GetVertexID()));
            if(m_vp->GetSocId() == -1)//it is in single
//                _log_root.error("MyGraph::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] Could not find node "+ std::to_string(current.GetVertexID()));
            continue;
        }
        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> pCurrNeighbors = iterNewNode->second->LoadIntervalNeighbors_Efficient(tStartInterval, tEndInterval);
        if (pCurrNeighbors != NULL)
        {
          //  fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
          //  fs << ("Node " + std::to_string(current.GetDepth()) + ", Neighbors size " + std::to_string(pCurrNeighbors->size())) << std::endl;
          //  fs.close();



            // Add results to the queue
            //
            for (int i = 0; i < pCurrNeighbors->size(); i++)
            {
                std::unique_ptr<NeighborMetaData> pNeighborMetaData = std::move(((*pCurrNeighbors)[i]));


                // add to the result
                //
                MyEdgeAdvanced newEdge(current.GetVertexID(), pNeighborMetaData->GetID(), pNeighborMetaData->GetEpoch(),
                                       lCurrDepth + 1, (i == pCurrNeighbors->size() - 1), pNeighborMetaData->GetCounter());// long lSource, long lDestination, tvg_time tEpoch, long lFrequency = 1
                pMapResult->push_back(newEdge);


                iterMarkedNodes = hashIDToDepth.find(pNeighborMetaData->GetID());
                if (iterMarkedNodes == hashIDToDepth.end()) // i.e. first visit
                {
                    VertexDepth newDepth(pNeighborMetaData->GetID(), lCurrDepth + 1);

            //        fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
              //      fs << ("Add to priority queue: Vertex " + std::to_string(newDepth.GetVertexID()) + ", Depth " +
//                                                              std::to_string(newDepth.GetDepth())) << std::endl;
                //    fs.close();

                    qVertices.push(newDepth);
                    hashIDToDepth[pNeighborMetaData->GetID()] = lCurrDepth + 1;

                }
                else
                {
                    if (iterMarkedNodes->second <= lCurrDepth + 1)
                    {
//                        fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
//                        fs << ("Skipped: Vertex " + std::to_string(pNeighborMetaData->GetID()) + ", Depth " +
//                                                    std::to_string(lCurrDepth + 1)) << std::endl;
//                        fs.close();

                        continue;
                    }
                    else // if the new node has a smaller depth - add it with the same value a a smaller key
                    {
                        VertexDepth newDepth(pNeighborMetaData->GetID(), lCurrDepth + 1);

//                        fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
//                        fs << ("Add to priority queue: Vertex " + std::to_string(newDepth.GetVertexID()) + ", Depth " +
//                                                                  std::to_string(newDepth.GetDepth())) << std::endl;
//                        fs.close();

                        qVertices.push(newDepth);
                        hashIDToDepth[pNeighborMetaData->GetID()] = lCurrDepth + 1;
                    }
                }


            }
        }
    }

    //_log_root.debug("MyGraph::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] End");
    return std::move(pMapResult);
}





/*
std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraph::BFS_Depth_Node_Efficient_NVRAM(const std::vector<tvg_vertex_id>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth)
{
    std::unique_ptr<std::list<MyEdgeAdvanced>> pMapResult = std::make_unique<std::list<MyEdgeAdvanced>>();

    if((iMaxDepth < 0) || (vNodes.empty()))
    {
        return pMapResult;
    }

    std::fstream fs("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out);
    fs<<("Begin BFS. Start time: " + tStartInterval.GetString() + ", End time " + tEndInterval.GetString() + ", Max Depth " + std::to_string(iMaxDepth) )<<std::endl;
    fs.close();


    // The queue of vertices
    //
    std::priority_queue<VertexDepth, std::vector<VertexDepth>, CompareVertexDepth> qVertices;


    // A hash (and an iterator) of marked nodes
    //
    std::map<tvg_counter, int> hashIDToDepth;
    std::map<tvg_counter, int>::iterator iterMarkedNodes;



    // Initialize
    //
    fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
    for (int i = 0 ; i < vNodes.size() ; i++)
    {
        VertexDepth newVertexDepth(vNodes[i], 0l);
        fs<<("Add to priority queue: Vertex " + std::to_string(newVertexDepth.GetVertexID())+ ", Depth " + std::to_string(newVertexDepth.GetDepth()))<<std::endl;
        qVertices.push(newVertexDepth);
        hashIDToDepth[vNodes[i]] = 0;
    }
    fs.close();




    std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterCurrNodeResults;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iterNewNode;




    // Main loop
    //
    for ( ; !qVertices.empty() ; qVertices.pop())
    {
        // shorted depth
        //
        const VertexDepth current(qVertices.top());
        int lCurrDepth = current.GetDepth();


        fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
        fs<<("Top priority queue: Vertex " + std::to_string(current.GetVertexID())+ ", Depth " + std::to_string(current.GetDepth()))<<std::endl;
        fs.close();



        if (lCurrDepth >= iMaxDepth)
            break;


        // if the same node was already inserted with a smaller depth - do not develop the node
        //
        iterMarkedNodes = hashIDToDepth.find(current.GetVertexID());
        if (iterMarkedNodes != hashIDToDepth.end())
        {
            if (iterMarkedNodes->second < current.GetDepth())
                continue;
        }


        // Locate the node and bring its neighbors
        //
        iterNewNode = m_mapIDNodes.find(current.GetVertexID());
        if (iterNewNode == m_mapIDNodes.end())
        {
            _log_root.error("Could not find node "+ std::to_string(current.GetVertexID()));
            continue;
        }

        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> pCurrNeighbors = iterNewNode->second->LoadIntervalNeighbors_Efficient_NVRAM(tStartInterval, tEndInterval,std::unique_ptr <SendUtils<> >());

        if (pCurrNeighbors != NULL)
        {
            fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
            fs << ("Node " + std::to_string(current.GetDepth()) + ", Neighbors size " + std::to_string(pCurrNeighbors->size())) << std::endl;
            fs.close();


            // Add results to the queue
            //
            for (int i = 0; i < pCurrNeighbors->size(); i++)
            {
                std::unique_ptr<NeighborMetaData> pNeighborMetaData = std::move(((*pCurrNeighbors)[i]));


                // add to the result
                //
                MyEdgeAdvanced newEdge(current.GetVertexID(), pNeighborMetaData->GetID(), pNeighborMetaData->GetEpoch(),
                                       pNeighborMetaData->GetCounter());// long lSource, long lDestination, tvg_time tEpoch, long lFrequency = 1
                pMapResult->push_back(newEdge);


                iterMarkedNodes = hashIDToDepth.find(pNeighborMetaData->GetID());
                if (iterMarkedNodes == hashIDToDepth.end()) // i.e. first visit
                {
                    VertexDepth newDepth(pNeighborMetaData->GetID(), lCurrDepth + 1);

                    fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
                    fs << ("Add to priority queue: Vertex " + std::to_string(newDepth.GetVertexID()) + ", Depth " +
                           std::to_string(newDepth.GetDepth())) << std::endl;
                    fs.close();

                    qVertices.push(newDepth);
                    hashIDToDepth[pNeighborMetaData->GetID()] = lCurrDepth + 1;

                }
                else
                {
                    if (iterMarkedNodes->second <= lCurrDepth + 1)
                    {
                        fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
                        fs << ("Skipped: Vertex " + std::to_string(pNeighborMetaData->GetID()) + ", Depth " +
                               std::to_string(lCurrDepth + 1)) << std::endl;
                        fs.close();

                        continue;
                    }
                    else // if the new node has a smaller depth - add it with the same value a a smaller key
                    {
                        VertexDepth newDepth(pNeighborMetaData->GetID(), lCurrDepth + 1);

                        fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
                        fs << ("Add to priority queue: Vertex " + std::to_string(newDepth.GetVertexID()) + ", Depth " +
                               std::to_string(newDepth.GetDepth())) << std::endl;
                        fs.close();

                        qVertices.push(newDepth);
                        hashIDToDepth[pNeighborMetaData->GetID()] = lCurrDepth + 1;
                    }
                }


            }
        }
    }


    return std::move(pMapResult);
}


 */




//TODO : decide which vertices are ready for eviction. Use BufferManager to evict memory
bool MyGraph::EvictMemory()
{

    _log_root.debug("MyGraph::EvictMemory Begin");
    //_log_root.warn("MyGraph::EvictMemory cause crashes ---- remarked");
    m_pGraphMemoryManager->ReleaseMemory(-1);//50*(1<<20) release 10MB
    _log_root.debug("MyGraph::EvictMemory End");
    return true;
}

/*
void process(Node node)
{
    // Do your own processing here. All nodes handed to
    // this method will be within the specified depth limit.
}
*/


//////////////////////

