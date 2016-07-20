//
// Created by norig on 12/24/15.
//



#include "../Header/MyGraphSelect.h"
#include <queue>
#include <unordered_set>


MyGraphSelect::MyGraphSelect(bool bDirected) : _log_root(log4cpp::Category::getRoot()), m_bDirected(bDirected), m_strGraphData("GraphData")
{

    m_vp = std::make_unique<VertexProxy>(-1);//define an invalid soc - need to call SetSoc when known

    MyVertexSelect::SetPerformInterpolationSearch(false);

//    LoadGraphInfo();
}



MyGraphSelect::~MyGraphSelect()
{


}


void MyGraphSelect::SetSocId(int soc_id)
{
    m_vp = std::make_unique<VertexProxy>(soc_id);
}



void MyGraphSelect::LoadGraphInfo()
{

    std::pair<tvg_long_counter, std::unique_ptr<tvg_vertex_id[]>> nvPair = BufferManager<TargetNVM>::Instance()->GetGenericVector<tvg_vertex_id>(m_strGraphData);
    std::unique_ptr<tvg_vertex_id[]> nvData = std::move(nvPair.second);

    for (tvg_long_counter i = 0 ; i < nvPair.first ; i++)
    {
        AddVertex(nvData[i]);
    }
}






bool MyGraphSelect::AddVertex(const tvg_vertex_id& lID)
{
//    _log_root.debug("MyGraphSelect::AddVertex Start");

    if (!lID.IsValid())//SAGI - remove the ref to Global with direct compute. This is not a configuration item. Probably need to be a function !!!
    {
        _log_root.warn("invalid liD "+std::to_string(lID));
        return false;
    }

    if (!ContainsVertex(lID))
    {
        std::unique_ptr<MyVertexSelect> pNewVertex = std::make_unique<MyVertexSelect>(lID);
        m_mapIDNodes[lID] = std::move(pNewVertex);
    }
    else
    {
        _log_root.error("MyGraph::AddVertex: Addidng existing vertex " + std::to_string(lID));
        return false;
    }

    return true;
}



std::unique_ptr<MyVertexSelect>& MyGraphSelect::GetVertex(tvg_vertex_id lID)
{
//    _log_root.debug("MyGraph::GetVertex Start");

    if (!ContainsVertex(lID))
    {
        AddVertex(lID);
    }

    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>>::iterator iter = m_mapIDNodes.find(lID);
    return iter->second;
}


bool MyGraphSelect::ContainsVertex(tvg_vertex_id lID)
{
    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>>::iterator iter = m_mapIDNodes.find(lID);
    return (iter != m_mapIDNodes.end());
}

/*
//std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> MyVertexSelect::LoadIntervalNeighbors_NVRAM(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch, const std::map<tvg_vertex_id, int>& vExcludeNodes)
std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraphSelect::LoadIntervalNeighbors(const std::vector<VertexDepth>& vNodes, const std::map<tvg_vertex_id, int>& vExcludeNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval)
{
    //_log_root.debug("MyGraphSelect::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] Start");
    std::unique_ptr<std::list<MyEdgeAdvanced>> pMapResult = std::make_unique<std::list<MyEdgeAdvanced>>();

    if(vNodes.empty())
    {
        return pMapResult;
    }



    std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterCurrNodeResults;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>>::iterator iterNewNode;


    // Main loop
    // we will want to make this loop multi threaded
    //
    for (int i = 0 ; i < vNodes.size() ; i++)
    {
        // do not develop the "vExcludeNodes"
        //
        std::map<tvg_vertex_id, int>::const_iterator iterExcludeNode = vExcludeNodes.find(vNodes[i].GetVertexID());
        if (iterExcludeNode != vExcludeNodes.end())
        {
            _log_root.warn("MyGraphSelect::KHop(with exsclusions): excluded node " + vNodes[i].GetVertexID().GetString() + " was in the input");
            continue;
        }

        if (!ContainsVertex(vNodes[i].GetVertexID()))
            AddVertex(vNodes[i].GetVertexID());

        VertexDepth newVertexDepth(vNodes[i].GetVertexID(), vNodes[i].GetDepth());
        //      fs<<("Add to priority queue: Vertex " + std::to_string(newVertexDepth.GetVertexID())+ ", Depth " + std::to_string(newVertexDepth.GetDepth()))<<std::endl;


        // shorted depth
        //
        const VertexDepth current(qVertices.top());
        int lCurrDepth = current.GetDepth();

        //_log_root.warn("working on vertex "+ std::to_string(current.GetVertexID()));

        //fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
        //fs<<("Top priority queue: Vertex " + std::to_string(current.GetVertexID())+ ", Depth " + std::to_string(current.GetDepth()))<<std::endl;
        //fs.close();



        // Check if the node is local
        //
        if (!m_vp->IsVertexLocal(current.GetVertexID()))
        {
            _log_root.info("MyGraphSelect::KHop["+std::to_string(m_vp->GetSocId())+"]: the vertex " +
                           std::to_string(current.GetVertexID()) + " is not local");

            continue;
        }



        // Locate the node and bring its neighbors
        //
        iterNewNode = m_mapIDNodes.find(current.GetVertexID());
        if (iterNewNode == m_mapIDNodes.end())
        {
            AddVertex(current.GetVertexID());
            iterNewNode = m_mapIDNodes.find(current.GetVertexID());
        }
        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> pCurrNeighbors = iterNewNode->second->LoadIntervalNeighbors_NVRAM(tStartInterval, tEndInterval, vExcludeNodes);
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

                // do not develop the "vExcludeNodes"
                //
                std::map<tvg_vertex_id, int>::const_iterator iterExcludeNode = vExcludeNodes.find(pNeighborMetaData->GetID());
                if (iterExcludeNode != vExcludeNodes.end())
                {
                    _log_root.error("MyGraphSelect::KHop(with exsclusions): excluded node " + pNeighborMetaData->GetID().GetString() + " returned from LoadIntervalNeighbors_NVRAM");
                    continue;
                }



                // Tst the model: removals etc.
                //
                if (!IsModelEdge(current.GetVertexID(), pNeighborMetaData, tStartInterval, tEndInterval))
                    continue;



                // add to the result
                //
                MyEdgeAdvanced newEdge(current.GetVertexID(), pNeighborMetaData->GetID(), pNeighborMetaData->GetEpoch(),
                                       lCurrDepth + 1, (i == pCurrNeighbors->size() - 1), pNeighborMetaData->GetCounter());// long lSource, long lDestination, tvg_time tEpoch, long lFrequency = 1
                pMapResult->push_back(newEdge);




            }
        }
    }

    //_log_root.debug("MyGraph::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] End");
    return std::move(pMapResult);
}
*/



std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraphSelect::LoadIntervalNeighbors(const std::vector<VertexDepth>& vNodes, const std::map<tvg_vertex_id, int>& vExcludeNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval)
{
    //_log_root.debug("MyGraphSelect::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] Start");
    std::unique_ptr<std::list<MyEdgeAdvanced>> pMapResult = std::make_unique<std::list<MyEdgeAdvanced>>();

    if(vNodes.empty())
    {
        return pMapResult;
    }


    std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterCurrNodeResults;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>>::iterator iterNewNode;


    // Main loop
    // we will want to make this loop multi threaded
    //
    //#pragma omp parallel
    //#pragma omp single nowait
    //{
    //then we need to add the task interface and the lock on the queue of vertices
    // Initialize
    //
    for (int i = 0 ; i < vNodes.size() ; i++)
    {
        std::map<tvg_vertex_id, int>::const_iterator iterExcludeNode = vExcludeNodes.find(vNodes[i].GetVertexID());
        if (iterExcludeNode != vExcludeNodes.end())
        {
            _log_root.warn("MyGraphSelect::LoadIntervalNeighbors(with exsclusions): excluded node " + vNodes[i].GetVertexID().GetString() + " was in the input");
            continue;
        }

        if (!ContainsVertex(vNodes[i].GetVertexID()))
            AddVertex(vNodes[i].GetVertexID());

        VertexDepth newVertexDepth(vNodes[i].GetVertexID(), vNodes[i].GetDepth());
        //      fs<<("Add to priority queue: Vertex " + std::to_string(newVertexDepth.GetVertexID())+ ", Depth " + std::to_string(newVertexDepth.GetDepth()))<<std::endl;


        // Check if the node is local
        //
        if (!m_vp->IsVertexLocal(vNodes[i].GetVertexID()))
        {
            _log_root.info("MyGraphSelect::LoadIntervalNeighbors["+std::to_string(m_vp->GetSocId())+"]: the vertex " +
                           std::to_string(vNodes[i].GetVertexID()) + " is not local");

            continue;
        }



        // Locate the node and bring its neighbors
        //
        iterNewNode = m_mapIDNodes.find(vNodes[i].GetVertexID());
        if (iterNewNode == m_mapIDNodes.end())
        {
            AddVertex(vNodes[i].GetVertexID());
            iterNewNode = m_mapIDNodes.find(vNodes[i].GetVertexID());
        }

        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> pCurrNeighbors = iterNewNode->second->LoadIntervalNeighbors_NVRAM(tStartInterval, tEndInterval, vExcludeNodes);
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

                std::map<tvg_vertex_id, int>::const_iterator iterExcludeNode = vExcludeNodes.find(pNeighborMetaData->GetID());
                if (iterExcludeNode != vExcludeNodes.end())
                {
                    _log_root.error("MyGraphSelect::KHop(with exsclusions): excluded node " + pNeighborMetaData->GetID().GetString() + " returned from LoadIntervalNeighbors_NVRAM");
                    continue;
                }

                if (!IsModelEdge(vNodes[i].GetVertexID(), pNeighborMetaData, tStartInterval, tEndInterval))
                    continue;


                MyEdgeAdvanced newEdge(vNodes[i].GetVertexID(), pNeighborMetaData->GetID(), pNeighborMetaData->GetEpoch(),
                                       0, (i == pCurrNeighbors->size() - 1), pNeighborMetaData->GetCounter());// lMyEdgeAdvanced(tvg_vertex_id lSource, tvg_vertex_id lDestination, tvg_time tEpoch, int iDepth, bool bIsLastEdge, long lFrequency)
                pMapResult->push_back(newEdge);
            }
        }
    }

    //_log_root.debug("MyGraph::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] End");
    return std::move(pMapResult);
}

std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraphSelect::KHop(const std::vector<VertexDepth>& vNodes, const std::map<tvg_vertex_id, int>& vExcludeNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth)
{
    //_log_root.debug("MyGraphSelect::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] Start");
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
        // do not develop the "vExcludeNodes"
        //
        std::map<tvg_vertex_id, int>::const_iterator iterExcludeNode = vExcludeNodes.find(vNodes[i].GetVertexID());
        if (iterExcludeNode != vExcludeNodes.end())
        {
            _log_root.warn("MyGraphSelect::KHop(with exsclusions): excluded node " + vNodes[i].GetVertexID().GetString() + " was in the input");
            continue;
        }

        if (!ContainsVertex(vNodes[i].GetVertexID()))
            AddVertex(vNodes[i].GetVertexID());

        VertexDepth newVertexDepth(vNodes[i].GetVertexID(), vNodes[i].GetDepth());
        //      fs<<("Add to priority queue: Vertex " + std::to_string(newVertexDepth.GetVertexID())+ ", Depth " + std::to_string(newVertexDepth.GetDepth()))<<std::endl;

        qVertices.push(newVertexDepth);
        hashIDToDepth[vNodes[i].GetVertexID()] = vNodes[i].GetDepth();

    }
    //fs.close();




    std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterCurrNodeResults;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>>::iterator iterNewNode;


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



        // Check if the node is local
        //
        if (!m_vp->IsVertexLocal(current.GetVertexID()))
        {
            _log_root.info("MyGraphSelect::KHop["+std::to_string(m_vp->GetSocId())+"]: the vertex " +
                           std::to_string(current.GetVertexID()) + " is not local");

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
            AddVertex(current.GetVertexID());
            iterNewNode = m_mapIDNodes.find(current.GetVertexID());
        }
        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> pCurrNeighbors = iterNewNode->second->LoadIntervalNeighbors_NVRAM(tStartInterval, tEndInterval, vExcludeNodes);
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

                // do not develop the "vExcludeNodes"
                //
                std::map<tvg_vertex_id, int>::const_iterator iterExcludeNode = vExcludeNodes.find(pNeighborMetaData->GetID());
                if (iterExcludeNode != vExcludeNodes.end())
                {
                    _log_root.error("MyGraphSelect::KHop(with exsclusions): excluded node " + pNeighborMetaData->GetID().GetString() + " returned from LoadIntervalNeighbors_NVRAM");
                    continue;
                }



                // Tst the model: removals etc.
                //
                if (!IsModelEdge(current.GetVertexID(), pNeighborMetaData, tStartInterval, tEndInterval))
                    continue;



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




std::unique_ptr<std::list<std::unique_ptr<std::list<MyEdgeAdvanced>>>> MyGraphSelect::SnapConnection(const MyEdge& e, const tvg_time& tStartInterval, const tvg_time& tEndInterval,
                                                                                                     const tvg_time& tSnapWindowSize, int iMaxDepth)
{
    std::unique_ptr<std::list<std::unique_ptr<std::list<MyEdgeAdvanced>>>> listOfSnaps = std::make_unique<std::list<std::unique_ptr<std::list<MyEdgeAdvanced>>>>();



    std::unique_ptr<MyVertexSelect>& pVSource = GetVertex(e.GetSource());


    //
    std::vector<VertexDepth> vNodes;
    vNodes.push_back(e.GetSource());
    vNodes.push_back(e.GetDestination());


    std::unique_ptr<std::list<tvg_time>> listOfEpochs = pVSource->GetLastConnectionAppearence(e.GetDestination(), tStartInterval, tEndInterval);

    std::list<tvg_time>::iterator iterEpoch = listOfEpochs->begin();
    for ( ; iterEpoch != listOfEpochs->end() ; iterEpoch++)
    {
        std::unique_ptr<std::list<MyEdgeAdvanced>> lRes = KHop(vNodes, (*iterEpoch), (*iterEpoch) + tSnapWindowSize, iMaxDepth);
        listOfSnaps->push_back(std::move(lRes));
    }





    return std::move(listOfSnaps);
}



std::unique_ptr<std::list<tvg_time>> MyGraphSelect::GetConnectionHistory(const MyEdge& e, const tvg_time& tStartInterval, const tvg_time& tEndInterval)
{
    _log_root.debug("MyGraphSelect::GetConnectionHistory start");
    if (m_vp->IsVertexLocal(e.GetSource()))
    {
        std::unique_ptr<MyVertexSelect>& pVSource = GetVertex(e.GetSource());
        return std::move(pVSource->GetLastConnectionAppearence(e.GetDestination(), tStartInterval, tEndInterval));
    }
    else
    {
        if (m_vp->IsVertexLocal(e.GetDestination()))
        {
            std::unique_ptr<MyVertexSelect>& pVSource = GetVertex(e.GetDestination());
            return std::move(pVSource->GetLastConnectionAppearence(e.GetSource(), tStartInterval, tEndInterval));
        }

        else
        {
            _log_root.info("MyGraphSelect::GetConnectionHistory: Both nodes in the edge (" + e.GetSource().GetString() + ", " + e.GetDestination().GetString() + ") are not local");
            return nullptr;
        }

    }






//    std::unique_ptr<MyVertexSelect>& pVDestination = GetVertex(e.GetDestination());


}



std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraphSelect::KHopIntersection(const std::vector<VertexDepth>& vNodes1, const tvg_time& tStartInterval1, const tvg_time& tEndInterval1, int iMaxDepth1,
                                                                           const std::vector<VertexDepth>& vNodes2, const tvg_time& tStartInterval2, const tvg_time& tEndInterval2, int iMaxDepth2)
{
    std::unique_ptr<std::list<MyEdgeAdvanced>> listEdges1 = KHop(vNodes1, tStartInterval1, tEndInterval1, iMaxDepth1);
    std::unique_ptr<std::list<MyEdgeAdvanced>> listEdges2 = KHop(vNodes2, tStartInterval2, tEndInterval2, iMaxDepth2);


    if (listEdges1->size() < listEdges2->size())
        return std::move(EdgeListIntersection(std::move(listEdges1), std::move(listEdges2)));
    else
        return std::move(EdgeListIntersection(std::move(listEdges2), std::move(listEdges1)));
}



// Assumed the first list is shorter
//
std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraphSelect::EdgeListIntersection(std::unique_ptr<std::list<MyEdgeAdvanced>> listEdges1, std::unique_ptr<std::list<MyEdgeAdvanced>> listEdges2)
{
    std::map<MyEdgeAdvanced, int, MyEdgeCompare> mapEdges;
    std::list<MyEdgeAdvanced>::iterator iterList1 = listEdges1->begin();
    for (int iDeb = 1 ; iterList1 != listEdges1->end() ; iterList1++, iDeb++)
    {
        mapEdges[*iterList1] = iDeb;
    }


    std::list<MyEdgeAdvanced>::iterator iterList2 = listEdges2->begin();
    for ( ; iterList2 != listEdges2->end() ; )
    {
        std::map<MyEdgeAdvanced, int, MyEdgeCompare>::iterator iterMap = mapEdges.find(*iterList2);
        if (iterMap == mapEdges.end())
        {
            iterList2 = listEdges2->erase(iterList2);
        }
        else
        {
            iterList2++;
        }
    }



    return std::move(listEdges2);
}


std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraphSelect::KHop(const std::vector<VertexDepth>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval, int iMaxDepth)
{
    //_log_root.debug("MyGraphSelect::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] Start");
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
        if (!ContainsVertex(vNodes[i].GetVertexID()))
            AddVertex(vNodes[i].GetVertexID());

        VertexDepth newVertexDepth(vNodes[i].GetVertexID(), vNodes[i].GetDepth());
        //      fs<<("Add to priority queue: Vertex " + std::to_string(newVertexDepth.GetVertexID())+ ", Depth " + std::to_string(newVertexDepth.GetDepth()))<<std::endl;
        qVertices.push(newVertexDepth);
        hashIDToDepth[vNodes[i].GetVertexID()] = vNodes[i].GetDepth();
    }
    //fs.close();




    std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterCurrNodeResults;
    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>>::iterator iterNewNode;


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



        // Check if the node is local
        //
        if (!m_vp->IsVertexLocal(current.GetVertexID()))
        {
            _log_root.info("MyGraphSelect::KHop["+std::to_string(m_vp->GetSocId())+"]: the vertex " +
                            std::to_string(current.GetVertexID()) + " is not local");

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
            AddVertex(current.GetVertexID());
            iterNewNode = m_mapIDNodes.find(current.GetVertexID());
        }

        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> pCurrNeighbors = iterNewNode->second->LoadIntervalNeighbors_NVRAM(tStartInterval, tEndInterval);
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


                if (!IsModelEdge(current.GetVertexID(), pNeighborMetaData, tStartInterval, tEndInterval))
                    continue;



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



std::unique_ptr<std::list<MyEdgeAdvanced>> MyGraphSelect::LoadIntervalNeighbors(const std::vector<VertexDepth>& vNodes, const tvg_time& tStartInterval, const tvg_time& tEndInterval)
{
    //_log_root.debug("MyGraphSelect::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] Start");
    std::unique_ptr<std::list<MyEdgeAdvanced>> pMapResult = std::make_unique<std::list<MyEdgeAdvanced>>();

    if(vNodes.empty())
    {
        return pMapResult;
    }


    std::map<tvg_vertex_id, std::unique_ptr<MyVertexSelect>>::iterator iterNewNode;


    // Main loop
    // we will want to make this loop multi threaded
    //
    //#pragma omp parallel
    //#pragma omp single nowait
    //{
    //then we need to add the task interface and the lock on the queue of vertices
    // Initialize
    //
    for (int i = 0 ; i < vNodes.size() ; i++)
    {
        if (!ContainsVertex(vNodes[i].GetVertexID()))
            AddVertex(vNodes[i].GetVertexID());

        VertexDepth newVertexDepth(vNodes[i].GetVertexID(), vNodes[i].GetDepth());
        //      fs<<("Add to priority queue: Vertex " + std::to_string(newVertexDepth.GetVertexID())+ ", Depth " + std::to_string(newVertexDepth.GetDepth()))<<std::endl;


        // Check if the node is local
        //
        if (!m_vp->IsVertexLocal(vNodes[i].GetVertexID()))
        {
            _log_root.info("MyGraphSelect::LoadIntervalNeighbors["+std::to_string(m_vp->GetSocId())+"]: the vertex " +
                           std::to_string(vNodes[i].GetVertexID()) + " is not local");

            continue;
        }



        // Locate the node and bring its neighbors
        //
        iterNewNode = m_mapIDNodes.find(vNodes[i].GetVertexID());
        if (iterNewNode == m_mapIDNodes.end())
        {
            AddVertex(vNodes[i].GetVertexID());
            iterNewNode = m_mapIDNodes.find(vNodes[i].GetVertexID());
        }

        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> pCurrNeighbors = iterNewNode->second->LoadIntervalNeighbors_NVRAM(tStartInterval, tEndInterval);


        if (pCurrNeighbors != NULL)
        {
            //  fs.open("/home/norig/tvg/Tvg4Tm/log/Output/Queue.txt", std::fstream::out | std::fstream::app);
            //  fs << ("Node " + std::to_string(current.GetDepth()) + ", Neighbors size " + std::to_string(pCurrNeighbors->size())) << std::endl;
            //  fs.close();



            // Add results to the queue
            //
            for (int j = 0; j < pCurrNeighbors->size(); j++)
            {
                std::unique_ptr<NeighborMetaData> pNeighborMetaData = std::move(((*pCurrNeighbors)[j]));

                if (!IsModelEdge(vNodes[i].GetVertexID(), pNeighborMetaData, tStartInterval, tEndInterval))
                    continue;


                MyEdgeAdvanced newEdge(vNodes[i].GetVertexID(), pNeighborMetaData->GetID(), pNeighborMetaData->GetEpoch(),
                                       0, (j == (pCurrNeighbors->size() - 1)), pNeighborMetaData->GetCounter());// lMyEdgeAdvanced(tvg_vertex_id lSource, tvg_vertex_id lDestination, tvg_time tEpoch, int iDepth, bool bIsLastEdge, long lFrequency)
                pMapResult->push_back(newEdge);
            }
        }
    }

    //_log_root.debug("MyGraph::BFS_Depth_Node_Efficient["+std::to_string(m_vp->GetSocId())+"] End");
    return std::move(pMapResult);
}



std::unique_ptr<AdditionsDeletionsLists> MyGraphSelect::GetAllNextConnections2(const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputNodes,
                                                                              const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputExcludeNodes,
                                                                              tvg_time tStartTime, tvg_time tEndTime,tvg_time tMaxWindowShift)
{
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vAddConnections = std::move(GetNextConnections(vInputNodes, vInputExcludeNodes, tEndTime + 1, tMaxWindowShift, true));
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vDelConnections = std::move(GetNextConnections(vInputNodes, vInputExcludeNodes, tStartTime, tMaxWindowShift, false));

    std::unique_ptr<AdditionsDeletionsLists> lists = std::make_unique<AdditionsDeletionsLists>(std::move(vAddConnections), std::move(vDelConnections));


    return std::move(lists);
}


std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> MyGraphSelect::GetAllNextConnections(const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputNodes,
                                                                                        const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputExcludeNodes,
                                                                                        tvg_time tStartTime, tvg_time tEndTime,tvg_time tMaxWindowShift)
{
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vAddConnections = std::move(GetNextConnections(vInputNodes, vInputExcludeNodes, tEndTime + 1, tMaxWindowShift, true));
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vDelConnections = std::move(GetNextConnections(vInputNodes, vInputExcludeNodes, tStartTime, tMaxWindowShift, false));



    return std::move(MergeLists(std::move(vAddConnections), std::move(vDelConnections), tEndTime - tStartTime));

}

std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> MyGraphSelect::GetNextConnections(const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputNodes,
                                                                                     const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputExcludeNodes,
                                                                                     tvg_time tTime, tvg_time tMaxWindowShift, bool bAddition)
{
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vOutput = std::make_unique<std::vector<std::unique_ptr<MyEdgeTemporalAction>>>();

    std::vector<tvg_vertex_id>::iterator iterInputNodes = vInputNodes->begin();
    for ( ; iterInputNodes != vInputNodes->end() ; iterInputNodes++)
    {
        tvg_vertex_id lVertexID = (*iterInputNodes);
        if (std::find(vInputExcludeNodes->begin(), vInputExcludeNodes->end(), lVertexID) != vInputExcludeNodes->end())
            continue;

        std::unique_ptr<MyVertexSelect>& pVertex = GetVertex(lVertexID);

        std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vCurrEdges = pVertex->GetNextConnections(vInputExcludeNodes, tTime, tMaxWindowShift, bAddition);

        /*
        for (int i = 0 ; i < vCurrEdges->size() ; i++)
        {
            std::cout<<"("<<(*(vCurrEdges))[i]->GetSource().GetString()<<", "<<(*(vCurrEdges))[i]->GetDestination().GetString()<<") "<<(*(vCurrEdges))[i]->GetEpoch().GetString()<<std::endl;
        }
        */

        if ((vCurrEdges != nullptr) && (vCurrEdges->size() > 0))
        {
            vOutput = MergeLists(std::move(vOutput), std::move(vCurrEdges), tvg_time(0));
        }
    }

    /*
    for (int i = 0 ; i < vOutput->size() ; i++)
    {
        std::cout<<"("<<(*(vOutput))[i]->GetSource().GetString()<<", "<<(*(vOutput))[i]->GetDestination().GetString()<<") "<<(*(vOutput))[i]->GetEpoch().GetString()<<std::endl;
    }
     */


    return std::move(vOutput);
}


std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> MyGraphSelect::MergeLists(std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vOriginal,
                                                                               std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vCurrEdges,
                                                                               tvg_time tShift)
{
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vMerged = std::make_unique<std::vector<std::unique_ptr<MyEdgeTemporalAction>>>();//vOriginal->size() + vCurrEdges->size());

    std::vector<std::unique_ptr<MyEdgeTemporalAction>>::iterator iterOriginal = vOriginal->begin();
    std::vector<std::unique_ptr<MyEdgeTemporalAction>>::iterator iterCurrEdges = vCurrEdges->begin();

    while(iterOriginal != vOriginal->end())
    {
        if (iterCurrEdges == vCurrEdges->end())
        {
            while(iterOriginal != vOriginal->end())
            {
                if ((vMerged->empty()) || (*(*iterOriginal)) != (*(vMerged->back())))
                {
                    vMerged->push_back(std::move(*iterOriginal));
                }

                iterOriginal = vOriginal->erase(iterOriginal);
            }

            break;
        }


        if ((*iterOriginal)->GetEpoch() <= (*iterCurrEdges)->GetEpoch() + tShift)
        {
            if ((vMerged->empty()) || (*(*iterOriginal)) != (*(vMerged->back())))
            {
                vMerged->push_back(std::move(*iterOriginal));
            }
            iterOriginal = vOriginal->erase(iterOriginal);
        }
        else
        {
            if ((vMerged->empty()) || (*(*iterCurrEdges)) != (*(vMerged->back())))
            {
                vMerged->push_back(std::move(*iterCurrEdges));
            }
            iterCurrEdges = vCurrEdges->erase(iterCurrEdges);
        }

    }

    int iDebugCounter = 0;
    if ((iterOriginal == vOriginal->end()) && (iterCurrEdges != vCurrEdges->end()))
    {
        for( ; iterCurrEdges != vCurrEdges->end() ; iDebugCounter++)
        {
            if ((vMerged->empty()) || (*(*iterCurrEdges)) != (*(vMerged->back())))
            {
                vMerged->push_back(std::move(*iterCurrEdges));
            }
            iterCurrEdges = vCurrEdges->erase(iterCurrEdges);
        }
    }


    return std::move(vMerged);
}



bool MyGraphSelect::IsModelEdge(tvg_vertex_id lCurrVertexID, std::unique_ptr<NeighborMetaData>& pNeighborMetaData, tvg_time tStartInterval, tvg_time tEndInterval)
{
    return true;
}