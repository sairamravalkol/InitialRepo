//
// Created by norig on 7/28/15.
//

#ifndef TVG4TM_GRAPHMEMORYMANAGER_H
#define TVG4TM_GRAPHMEMORYMANAGER_H


#include <map>
#include <log4cpp/Category.hh>
#include "MyVertex.h"
#include "../../../infrastructure/BufferManager.h"
//#include "../../../infrastructure/TargetTextFile.h"
#include "../../../infrastructure/TargetNVM.h"


//class MyVertex;



enum MemoryArrayType {ARRAY_VALUES, ARRAY_CHAIN, ARRAY_ADDITIONAL_CHAIN};//TODO repalce with enum class



class VertexMemoryData
{
private:
    //
    tvg_index m_lNewValuesAdded;
    tvg_index m_lChainSize;
    tvg_index m_lAdditionalChainSize;


    tvg_long_counter  m_lTotalMemoryAllocated;


public:
    //
    VertexMemoryData()
    {
        Reset();
    }

    VertexMemoryData(tvg_index lNewValuesAdded, tvg_index lNewChainAdded, tvg_index lAdditionalChainSize)
    {
        m_lNewValuesAdded = lNewValuesAdded;
        m_lChainSize = lNewChainAdded;
        m_lAdditionalChainSize = lAdditionalChainSize;

//        m_lTotalMemoryAllocated += m_lNewValuesAdded * sizeof() + lCompleteChainSize * sizeof();
    }


    void IncreaseNewValuesAdded(tvg_index lNewValuesAdded)
    {
        m_lNewValuesAdded += lNewValuesAdded;
    }

    void IncreaseChainSize(tvg_index lNewChainAdded)
    {
        m_lChainSize += lNewChainAdded;
    }

    void IncreaseAdditionalChainSize(tvg_index lAdditionalChainSize)
    {
        m_lAdditionalChainSize += lAdditionalChainSize;
    }



    void Reset()
    {
        m_lNewValuesAdded = 0;
        m_lChainSize = 0;
        m_lAdditionalChainSize = 0;

        m_lTotalMemoryAllocated = 0;
    }




    tvg_index GetNewValuesAdded()
    {
        return m_lNewValuesAdded;
    }
    tvg_index GetChainSize()
    {
        return m_lChainSize;
    }
    tvg_index GetAdditionalChainSize()
    {
        return m_lAdditionalChainSize;
    }



};



class MemoryHeapStructure
{
protected:
    //
    tvg_vertex_id m_lVertexID;
//    MemoryArrayType m_eArrayType;



public:
    //
    MemoryHeapStructure()
    {

    }

    MemoryHeapStructure(const tvg_vertex_id& lID) : m_lVertexID(lID)//, m_eArrayType(eArrayType)
    {

    }


    virtual ~MemoryHeapStructure()
    {

    }


    const tvg_vertex_id& GetVertexID() const
    {
        return m_lVertexID;
    }

    void SetData(const tvg_vertex_id& lID)
    {
        m_lVertexID = lID;
    }

    /*
    const MemoryArrayType& GetArrayType() const
    {
        return m_eArrayType;
    }

    void SetData(const tvg_vertex_id& lID, MemoryArrayType eArrayType)
    {
        m_lVertexID = lID;
        m_eArrayType = eArrayType;
    }
     */
};


class CompareMemoryHeapStructure
{
public:
    //
    bool operator()(const MemoryHeapStructure& v1, const MemoryHeapStructure& v2) const
    {
//        if (v1.GetArrayType() == v2.GetArrayType())
//        {
            return (v1.GetVertexID() < v2.GetVertexID());
//        }
//        else
//            return (v1.GetArrayType() < v2.GetArrayType());
    }
};









class MemoryHeapStructureAllocated : public MemoryHeapStructure
{
private:
    //
    tvg_long_counter m_lMemoryAllocated;




public:
    //
    MemoryHeapStructureAllocated()
    {

    }

    MemoryHeapStructureAllocated(const tvg_vertex_id& lID, tvg_long_counter lMemoryAllocated) : MemoryHeapStructure(lID), m_lMemoryAllocated(lMemoryAllocated)
    {

    }

    virtual ~MemoryHeapStructureAllocated()
    {

    }


    MemoryHeapStructureAllocated(const MemoryHeapStructureAllocated& other)
    {
        m_lVertexID = other.m_lVertexID;
 //       m_eArrayType = other.m_eArrayType;

        m_lMemoryAllocated = other.m_lMemoryAllocated;
    }


    void SetData(const tvg_vertex_id& lID, MemoryArrayType eArrayType, tvg_long_counter lMemoryAllocated)
    {
        MemoryHeapStructure::SetData(lID);
        m_lMemoryAllocated = lMemoryAllocated;
    }

    const MemoryHeapStructureAllocated& operator=(const MemoryHeapStructureAllocated& other)
    {
        m_lVertexID = other.m_lVertexID;
//        m_eArrayType = other.m_eArrayType;

        m_lMemoryAllocated = other.m_lMemoryAllocated;
    }

    inline const tvg_long_counter GetMemoryAllocated() const
    {
        return m_lMemoryAllocated;
    }


    void IncreaseMemoryAllocated(tvg_long_counter lMemoryAllocated)
    {
        m_lMemoryAllocated += lMemoryAllocated;
    }

    void SetMemoryAllocated(tvg_long_counter lMemoryAllocated)
    {
        m_lMemoryAllocated = lMemoryAllocated;
    }
};




class CompareMemoryHeapStructureAllocated
{
public:
    //
    bool operator()(const MemoryHeapStructureAllocated& v1, const MemoryHeapStructureAllocated& v2) const
    {
        if (v1.GetMemoryAllocated() == v2.GetMemoryAllocated())
        {
//            if (v1.GetArrayType() == v2.GetArrayType())
//            {
                return (v1.GetVertexID() < v2.GetVertexID());
//            }
//            else
//                return (v1.GetArrayType() < v2.GetArrayType());
        }
        else
        {
            return (v1.GetMemoryAllocated() > v2.GetMemoryAllocated());
        }
    }
};




























































///////////////////////////////////////////////////////////////////////////////////////
//
class GraphMemoryManager
{
private:
    //
    // Boolean to decide whether to release all the node data if needed, or each array for itself
    //
    bool m_bSubmitVertexData;



    // For debugging - releasing an array if the condition exists
    //
    bool m_bDebugMode;
    tvg_long_counter m_lMaxArraySize;
    double m_dLastPrintedMemorySize;
    const double m_dPrintThreshold;


//    std::map<tvg_vertex_id, std::unique_ptr<VertexMemoryData>> m_mapIDVertexMemory;


//    const tvg_vertex_id m_lSavedID;

    const std::string m_strGraphData;

    std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>& m_mapIDNode;


    log4cpp::Category& m_log_root;

    BufferManager<TargetNVM>* m_bBufferManager;



    double m_dGraphMemoryAllocated;
    std::vector<MemoryHeapStructureAllocated> m_vMemoryHeap;
    std::map<tvg_vertex_id, tvg_counter> m_mapIdToIndex;


    const tvg_long_counter m_lSizeofNVVertex;

    const tvg_long_counter m_lSizeofAdjacentNVVertex;
    const tvg_long_counter m_lSizeofNVChainStructure;
    const tvg_long_counter m_lSizeofNVTemporalLink;


//    BufferManager<TargetTextFile>* m_bBufferManager2;



 //   std::priority_queue<MemoryHeapStructure, std::vector<MemoryHeapStructure>, CompareMemoryHeapStructure> m_queueMemoryAllocated;




public:
    //
    GraphMemoryManager(std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>& mapIDNode) : m_mapIDNode(mapIDNode), m_log_root(log4cpp::Category::getRoot()), m_lSizeofAdjacentNVVertex(sizeof(AdjacentNVVertex)), m_lSizeofNVChainStructure(sizeof(NVChainStructure)), m_lSizeofNVTemporalLink(sizeof(SameNodeNVTemporalLink)), m_lSizeofNVVertex(sizeof(AdjacentNVVertex) + sizeof(NVChainStructure)), /*m_lSavedID(-1),*/ m_dPrintThreshold(0.3), m_strGraphData("GraphData")
    {
        // Eyal
        m_lMaxArraySize = 1; //Configuration::Instance()->GetIntegerItem("data-structure.max-submit-size");
        m_bBufferManager = BufferManager<TargetNVM>::Instance();

        m_dGraphMemoryAllocated = 0.0;
        m_dLastPrintedMemorySize = 1;
        m_bDebugMode = true;

    }




    //






    /*
    void AddNode(tvg_vertex_id lID)
    {
        std::map<tvg_vertex_id, std::unique_ptr<VertexMemoryData>>::iterator iter = m_mapIDVertexMemory.find(lID);
        if (iter == m_mapIDVertexMemory.end())
        {
            std::unique_ptr<VertexMemoryData> pVertexMemoryData = std::make_unique<VertexMemoryData>();
            m_mapIDVertexMemory.insert(std::pair<tvg_vertex_id, std::unique_ptr<VertexMemoryData>>(lID, std::move(pVertexMemoryData)));

        }
        else
        {
            m_log_root.warn("GraphMemoryManager.AddNode: the node " + std::to_string(lID) + " exists");
        }
    }
*/


    tvg_long_counter ReleaseMemory(const tvg_long_counter lMemoryAmountToRelease = -1)
    {
        tvg_long_counter lMemoryReleased = 0;
        tvg_long_counter lReleaseIndex;

        while ((lMemoryAmountToRelease == -1) || (lMemoryReleased < lMemoryAmountToRelease))
        {
            if (m_vMemoryHeap.size() == 0 || m_vMemoryHeap[0].GetMemoryAllocated() <= 0)
                break;

//            if (m_bSubmitVertexData)
//            {
//                lMemoryReleased += SubmitVertexData(m_vMemoryHeap[lReleaseIndex].GetVertexID());

//            }
//            else
//            {
                lMemoryReleased += SubmitVertexData(0);

//            }
        }

        return lMemoryReleased;
    }


    /*
    tvg_long_counter SubmitVertexData(tvg_vertex_id lVertexID)
    {
        tvg_long_counter lMemoryReleased = 0;


        lMemoryReleased += SubmitArray(m_mapIdToIndex[lVertexID]);
 //       lMemoryReleased += SubmitArray(m_mapIdToIndex[lVertexID][ARRAY_CHAIN], ARRAY_CHAIN);
 //     lMemoryReleased += SubmitArray(m_mapIdToIndex[lVertexID][ARRAY_ADDITIONAL_CHAIN]);



        return lMemoryReleased;

    }
     */


    tvg_long_counter SubmitVertexData(tvg_long_counter lReleaseIndex)
    {
        tvg_long_counter lMemoryReleased = 0;

        tvg_long_counter lIndex = MarkReleasedData(lReleaseIndex, lMemoryReleased);


        // First Submit the vertices, then the chain
        //
        if (lIndex != -1)
        {
            std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iterNode = m_mapIDNode.find(m_vMemoryHeap[lIndex].GetVertexID());
            if (iterNode == m_mapIDNode.end())
            {
                m_log_root.error("GraphMemoryManager::SubmitVertexData: the id " + std::to_string(m_vMemoryHeap[lIndex].GetVertexID()) + " not found in the nodes list");
            }

            else
            {
//                iterNode->second->SetIsSubmitted(true);




                tvg_long_counter lElementsCont = iterNode->second->GetCompletedVerticesElementsCount();
 //               m_dGraphMemoryAllocated -= lElementsCont * m_lSizeofAdjacentNVVertex;
                m_bBufferManager->Submit(iterNode->first, iterNode->second->SubmitNVAdjacencyList(), lElementsCont);


                lElementsCont = iterNode->second->GetCompletedVerticesElementsCount();
                std::unique_ptr<NVChainStructure[]> submitted = iterNode->second->SubmitChains();


                iterNode->second->ResetVertecesAndChains();


                if (DoSubmit(submitted, lElementsCont))
                {
                    m_bBufferManager->Submit(iterNode->first, std::move(submitted), lElementsCont);
                }

//                m_dGraphMemoryAllocated -= lElementsCont * m_lSizeofNVChainStructure;

                m_dGraphMemoryAllocated -= lElementsCont;

                /*
                            m_bBufferManager->Submit(iterNode->first, iterNode->second->SubmitAdditionalChains(),
                                                            iterNode->second->GetCurrAdditionalChainElementsCount());

               */

                if (m_bDebugMode)
                {
                    if (std::abs(m_dLastPrintedMemorySize - m_dGraphMemoryAllocated) * m_dPrintThreshold > m_dLastPrintedMemorySize)
                    {
//                        std::cout<<"The new graph memory allocated: "<<m_dGraphMemoryAllocated<<std::endl;
                        m_dLastPrintedMemorySize = m_dGraphMemoryAllocated;
                    }

                }
            }

        }

        return lMemoryReleased;
    }


    bool DoSubmit(std::unique_ptr<NVChainStructure[]>& submitted, tvg_index lSize)
    {
        for (tvg_index i = 0 ; i < lSize ; i++)
        {
            if (submitted[i].GetRealSize() > 0)
                return true;
        }

        return false;
    }

    tvg_long_counter MarkReleasedData(tvg_long_counter lChangedIndex, tvg_long_counter& lMemoryReleased)
    {
        if (m_vMemoryHeap[lChangedIndex].GetMemoryAllocated() <= 0)
            return -1;


        tvg_long_counter lIndex;

        lMemoryReleased += m_vMemoryHeap[lChangedIndex].GetMemoryAllocated();
        m_vMemoryHeap[lChangedIndex].SetMemoryAllocated(0);
        lIndex = SiftRight(lChangedIndex);
        m_mapIdToIndex[m_vMemoryHeap[lIndex].GetVertexID()] = lIndex;

        return lIndex;
    }


/*
    void SubmitData(tvg_long_counter lIndex)
    {
        std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iterNode = m_mapIDNode.find(m_vMemoryHeap[lIndex].GetVertexID());
        if (iterNode == m_mapIDNode.end())
        {
            m_log_root.error("GraphMemoryManager::ResetNode: the id " + std::to_string(m_vMemoryHeap[lIndex].GetVertexID()) + " not found in the nodes list");
        }

        else
        {
//                iterNode->second->SetIsSubmitted(true);


            m_bBufferManager->Submit(iterNode->first, iterNode->second->SubmitNVAdjacencyList(),
                                             iterNode->second->GetCompletedVerticesElementsCount());


            m_bBufferManager->Submit(iterNode->first, iterNode->second->SubmitChains(),
                                             iterNode->second->GetCompletedVerticesElementsCount());




//             m_bBufferManager->Submit(iterNode->first, iterNode->second->SubmitAdditionalChains(),
//                                             iterNode->second->GetCurrAdditionalChainElementsCount());


        }
    }
*/






    void UpdateNode(tvg_vertex_id lID, tvg_index lNewValuesAdded, tvg_index lNewChainAdded, tvg_index lAdditionalChainSize)
    {

        m_bDebugMode = true;


        // Since in the current version we are submitting all the node data, the chain is submitted together with the vertices info
        //
        if (!lNewValuesAdded.IsValid() || (lNewValuesAdded <= tvg_index(0)))
            return;




/*
        std::map<tvg_vertex_id, std::unique_ptr<VertexMemoryData>>::iterator iter = m_mapIDVertexMemory.find(lID);
        std::unique_ptr<VertexMemoryData> pVertexMemoryData;
        if (iter == m_mapIDVertexMemory.end())
        {
 //           m_log_root.warn("GraphMemoryManager.UpdateNode: the node " + std::to_string(lID) + " does not exist");

            pVertexMemoryData = std::make_unique<VertexMemoryData>(lNewValuesAdded, lNewChainAdded, lAdditionalChainSize);
            m_mapIDVertexMemory[lID] = std::move(pVertexMemoryData);

            iter = m_mapIDVertexMemory.find(lID);
        }
        else
        {
            iter->second->IncreaseNewValuesAdded(lNewValuesAdded);
            iter->second->IncreaseChainSize(lNewChainAdded);
            iter->second->IncreaseAdditionalChainSize(lAdditionalChainSize);
        }
*/


        // Update the memory ordering info
        //
        // Find it in the map
        //
        tvg_long_counter lIndex;
        std::map<tvg_vertex_id, tvg_counter>::iterator iterMapIdToIndex = m_mapIdToIndex.find(lID);
        if (iterMapIdToIndex == m_mapIdToIndex.end()) // first time addition
        {
 //           m_dGraphMemoryAllocated += lNewValuesAdded * m_lSizeofNVVertex;
            m_dGraphMemoryAllocated += lNewValuesAdded;

            MemoryHeapStructureAllocated memoryHeapStructure(lID, lNewValuesAdded);// * m_lSizeofNVVertex);

            m_vMemoryHeap.push_back(memoryHeapStructure);
            lIndex = SiftLeft(m_vMemoryHeap.size() - 1);
            m_mapIdToIndex[lID] = lIndex;




            /*
            MemoryHeapStructureAllocated memoryHeapStructure;

            //ARRAY_VALUES
            //
            if (lNewValuesAdded.IsValid() && (lNewValuesAdded > tvg_index(0)))
            {
                memoryHeapStructure.SetData(lID, ARRAY_VALUES, lNewValuesAdded * lSizeofAdjacentNVVertex);

            }
            else
            {
                memoryHeapStructure.SetData(lID, ARRAY_VALUES, 0);
            }
            m_vMemoryHeap.push_back(memoryHeapStructure);
            lIndex = SiftLeft(m_vMemoryHeap.size() - 1);
            m_mapIdToIndex[lID][ARRAY_VALUES] = lIndex;



            // ARRAY_CHAIN
            //
            if (lNewChainAdded.IsValid() && (lNewChainAdded > tvg_index(0)))
            {
                memoryHeapStructure.SetData(lID, ARRAY_CHAIN, lNewChainAdded * lSizeofNVChainStructure);
            }
            else
            {
                memoryHeapStructure.SetData(lID, ARRAY_CHAIN, 0);
            }
            m_vMemoryHeap.push_back(memoryHeapStructure);
            lIndex = SiftLeft(m_vMemoryHeap.size() - 1);
            m_mapIdToIndex[lID][ARRAY_CHAIN] = lIndex;


            // ARRAY_ADDITIONAL_CHAIN
            //
            if (lAdditionalChainSize.IsValid() && (lAdditionalChainSize > tvg_index(0)))
            {
                memoryHeapStructure.SetData(lID, ARRAY_ADDITIONAL_CHAIN, lAdditionalChainSize * lSizeofNVTemporalLink);
            }
            else
            {
                memoryHeapStructure.SetData(lID, ARRAY_ADDITIONAL_CHAIN, 0);
            }
            m_vMemoryHeap.push_back(memoryHeapStructure);
            lIndex = SiftLeft(m_vMemoryHeap.size() - 1);
            m_mapIdToIndex[lID][ARRAY_ADDITIONAL_CHAIN] = lIndex;
             */
        }
        else
        {
            if (lNewValuesAdded.IsValid() && (lNewValuesAdded > tvg_index(0)))
            {
//                m_dGraphMemoryAllocated += lNewValuesAdded * m_lSizeofNVVertex;
                m_dGraphMemoryAllocated += lNewValuesAdded;

                m_vMemoryHeap[iterMapIdToIndex->second].IncreaseMemoryAllocated(lNewValuesAdded);// * m_lSizeofNVVertex);
                lIndex = SiftLeft(iterMapIdToIndex->second);
                m_mapIdToIndex[iterMapIdToIndex->first] = lIndex;
            }




            /*
            if (lNewValuesAdded.IsValid() && (lNewValuesAdded > tvg_index(0)))
            {
                m_vMemoryHeap[(iterMapIdToIndex->second[ARRAY_VALUES])].IncreaseMemoryAllocated(lNewValuesAdded * lSizeofAdjacentNVVertex);
                lIndex = SiftRight(iterMapIdToIndex->second[ARRAY_VALUES]);
                iterMapIdToIndex->second[ARRAY_VALUES] = lIndex;
            }


            if (lNewChainAdded.IsValid() && (lNewChainAdded > tvg_index(0)))
            {
                m_vMemoryHeap[(iterMapIdToIndex->second[ARRAY_CHAIN])].IncreaseMemoryAllocated(lNewChainAdded * lSizeofNVChainStructure);
                lIndex = SiftRight(iterMapIdToIndex->second[ARRAY_CHAIN]);
                iterMapIdToIndex->second[ARRAY_CHAIN] = lIndex;
            }


            if (lAdditionalChainSize.IsValid() && (lAdditionalChainSize > tvg_index(0)))
            {
                m_vMemoryHeap[(iterMapIdToIndex->second[ARRAY_ADDITIONAL_CHAIN])].IncreaseMemoryAllocated(lAdditionalChainSize * lSizeofNVTemporalLink);
                lIndex = SiftRight(iterMapIdToIndex->second[ARRAY_ADDITIONAL_CHAIN]);
                iterMapIdToIndex->second[ARRAY_ADDITIONAL_CHAIN] = lIndex;
            }
             */
        }

        if (m_bDebugMode)
        {
            if (std::abs(m_dLastPrintedMemorySize - m_dGraphMemoryAllocated) * m_dPrintThreshold > m_dLastPrintedMemorySize)
            {
                double ddd = std::abs(m_dLastPrintedMemorySize - m_dGraphMemoryAllocated) * m_dPrintThreshold;
 //               std::cout<<"The new graph memory allocated: "<<m_dGraphMemoryAllocated<<std::endl;
                m_dLastPrintedMemorySize = m_dGraphMemoryAllocated;
            }

        }


        // Check the max values and release if is over the threshold
        //
        while (m_bDebugMode && (m_vMemoryHeap[0].GetMemoryAllocated() > m_lMaxArraySize))
        {
            ReleaseMemory(m_lMaxArraySize);
        }

    }


/*
    tvg_long_counter SiftLeft(tvg_long_counter lCurrIndex)
    {
        if ((lCurrIndex < 0) || (lCurrIndex >= m_vMemoryHeap.size()))
            return -1;

        for (int i = lCurrIndex ; i > 0 ; i--)
        {
            if (m_vMemoryHeap[i].GetMemoryAllocated() >= m_vMemoryHeap[i-1].GetMemoryAllocated())
                return i;

            m_mapIdToIndex[m_vMemoryHeap[i-1].GetVertexID()] = i;
            SwapHeapArrayValues(i-1, i);
        }


        return 0;
    }
*/



    tvg_long_counter SiftRight(tvg_long_counter lCurrIndex)
    {
        int lTotalSize = m_vMemoryHeap.size();
        if ((lCurrIndex < 0) || (lCurrIndex >= lTotalSize))
            return -1;

        int lLeft;
        int lRight;

        for (int i = lCurrIndex ; i >= 0 ; )
        {
            lLeft = i * 2 + 1;
            lRight = lLeft + 1;

            if ((lRight >= lTotalSize) && (lLeft >= lTotalSize))
                return i;

            if (lRight < lTotalSize)
            {
                if (lLeft >= lTotalSize)
                {
                    if (m_vMemoryHeap[i].GetMemoryAllocated() >= m_vMemoryHeap[lRight].GetMemoryAllocated())
                        return i;
                    else
                    {
                        m_mapIdToIndex[m_vMemoryHeap[lRight].GetVertexID()] = i;
                        SwapHeapArrayValues(lRight, i);
                        i = lRight;
                    }
                }
                else // both are in range
                {
                    if (m_vMemoryHeap[i].GetMemoryAllocated() >= m_vMemoryHeap[lRight].GetMemoryAllocated())
                    {
                        if (m_vMemoryHeap[i].GetMemoryAllocated() >= m_vMemoryHeap[lLeft].GetMemoryAllocated())
                            return i;
                        else
                        {
                            m_mapIdToIndex[m_vMemoryHeap[lLeft].GetVertexID()] = i;
                            SwapHeapArrayValues(lLeft, i);
                            i = lLeft;
                        }
                    }
                    else
                    {
                        if (m_vMemoryHeap[i].GetMemoryAllocated() >= m_vMemoryHeap[lLeft].GetMemoryAllocated())
                        {
                            m_mapIdToIndex[m_vMemoryHeap[lRight].GetVertexID()] = i;
                            SwapHeapArrayValues(lRight, i);
                            i = lRight;
                        }
                        else // both are bigger
                        {
                            if (m_vMemoryHeap[lLeft].GetMemoryAllocated() > m_vMemoryHeap[lRight].GetMemoryAllocated()) // go left
                            {
                                m_mapIdToIndex[m_vMemoryHeap[lLeft].GetVertexID()] = i;
                                SwapHeapArrayValues(lLeft, i);
                                i = lLeft;
                            }
                            else
                            {
                                m_mapIdToIndex[m_vMemoryHeap[lRight].GetVertexID()] = i;
                                SwapHeapArrayValues(lRight, i);
                                i = lRight;
                            }
                        }
                    }

                }
            }
            else
            {
                if (m_vMemoryHeap[i].GetMemoryAllocated() >= m_vMemoryHeap[lLeft].GetMemoryAllocated())
                    return i;
                else
                {
                    m_mapIdToIndex[m_vMemoryHeap[lLeft].GetVertexID()] = i;
                    SwapHeapArrayValues(lLeft, i);
                    i = lLeft;
                }
            }





        }


        return 0;
    }


    tvg_long_counter SiftLeft(tvg_long_counter lCurrIndex)
    {
        int lTotalSize = m_vMemoryHeap.size();
        if ((lCurrIndex < 0) || (lCurrIndex >= lTotalSize))
            return -1;

        int lParent;
        int k = lCurrIndex;
        for ( ; k > 0 ; k = lParent)
        {
            lParent = (k - 1) / 2;

            if (lParent < 0)
                return k;


            if (m_vMemoryHeap[k].GetMemoryAllocated() <= m_vMemoryHeap[lParent].GetMemoryAllocated())
                return k;

            m_mapIdToIndex[m_vMemoryHeap[lParent].GetVertexID()] = k;
            SwapHeapArrayValues(k, lParent);
        }


        return 0;
    }


/*
    tvg_long_counter SiftRight(tvg_long_counter lCurrIndex)
    {
        if ((lCurrIndex < 0) || (lCurrIndex >= m_vMemoryHeap.size()))
            return -1;

        for (int i = lCurrIndex ; i < m_vMemoryHeap.size() - 1 ; i++)
        {
            if (m_vMemoryHeap[i].GetMemoryAllocated() <= m_vMemoryHeap[i+1].GetMemoryAllocated())
                return i;

            m_mapIdToIndex[m_vMemoryHeap[i+1].GetVertexID()] = i;
            SwapHeapArrayValues(i, i + 1);
        }


        return (m_vMemoryHeap.size() - 1);
    }
*/

    bool IsMemoryHeapCorrect()
    {
        int lParent;
        int lTotalSize = m_vMemoryHeap.size();

        for (int k = lTotalSize - 1 ; k > 0 ; k--)
        {

            lParent = (k - 1) / 2;

            if (lParent < 0)
                break;

            if (m_vMemoryHeap[k].GetMemoryAllocated() > m_vMemoryHeap[lParent].GetMemoryAllocated())
                return false;
        }

        return true;
    }


    void SwapHeapArrayValues(tvg_long_counter l1, tvg_long_counter l2)
    {
        MemoryHeapStructureAllocated sTest = m_vMemoryHeap[l1];
        m_vMemoryHeap[l1] = m_vMemoryHeap[l2];
        m_vMemoryHeap[l2] = sTest;
    }


    template<class T> std::unique_ptr<T[]> Get(int vertex_id,int offset, int num_items)
    {
        return  std::move(m_bBufferManager->Get<T>(vertex_id, offset,  num_items));
    }



  /*

    tvg_counter SubmitNode(tvg_vertex_id lID)
    {
        tvg_counter lRes = 0;

        // Reset the methadata
        //
        std::map<tvg_vertex_id, std::unique_ptr<std::map<MemoryArrayType, tvg_counter>>> iter = m_mapIdToIndex.find(lID);
        std::unique_ptr<VertexMemoryData> pVertexMemoryData;
        if (iter == m_mapIdToIndex.end())
        {
            m_log_root.warn("GraphMemoryManager.ResetNode: the node " + std::to_string(lID) + " does not exist");

            pVertexMemoryData = std::make_unique<VertexMemoryData>();
            m_mapIDVertexMemory[lID] = std::move(pVertexMemoryData);
        }
        else
        {
            // Copy the data
            //
            std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iterNode = m_mapIDNode.find(lID);
            if (iterNode == m_mapIDNode.end())
            {
                m_log_root.error("GraphMemoryManager::ResetNode: the id " + std::to_string(lID) + " not found in the nodes list");
            }

            else
            {
                iterNode->second->SetIsSubmitted(true);

                lRes += m_bBufferManager->Submit(lID, iterNode->second->SubmitNVAdjacencyList(), iterNode->second->GetCompletedVerticesElementsCount());
                lRes += m_bBufferManager->Submit(lID, iterNode->second->SubmitChains(), iterNode->second->GetCurrChainElementsCount());

                lRes += m_bBufferManager->Submit(lID, iterNode->second->SubmitAdditionalChains(), iterNode->second->GetCurrAdditionalChainElementsCount());
                lRes += m_bBufferManager->Submit(lID, iterNode->second->SubmitMapAdditionalNodesToChains(), iterNode->second->GetCurrMapAdditionalNodesToChainsElementsCount());

                lRes += m_bBufferManager->Submit(lID, iterNode->second->SubmitMetadata(), iterNode->second->GetCurrMetadataSize());

            }



            iter->second->Reset();
        }

        return lRes;
    }


    tvg_counter SubmitAll()
    {
        m_bBufferManager->PrepareStorage();

        tvg_counter lReleaseSize = 0;
        std::map<tvg_vertex_id, std::unique_ptr<std::map<MemoryArrayType, tvg_counter>>>::iterator iter = m_mapIdToIndex.begin();
        for ( ; iter !=  m_mapIdToIndex.end() ; iter++)
        {
            lReleaseSize += SubmitNode(iter->first);
        }

        return lReleaseSize;
    }
*/

  void SubmitAll()
  {
//      m_bBufferManager->PrepareStorage();


      // First Release all the arrays
      //
      ReleaseMemory();



      // Release the rest of data
      //
      tvg_long_counter lElementsCont;
      std::map<tvg_vertex_id, tvg_counter>::iterator iterMap = m_mapIdToIndex.begin();
      for ( ; iterMap != m_mapIdToIndex.end() ; iterMap++)
      {

          std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iterNode = m_mapIDNode.find(iterMap->first);
          if (iterNode == m_mapIDNode.end())
          {
              m_log_root.error("GraphMemoryManager::ResetNode: the id " + std::to_string(iterMap->first) + " not found in the nodes list");
          }

          else
          {
              iterNode->second->SetIsSubmitted(true);

              tvg_vertex_id lDebug = iterMap->first;

//              m_lGraphMemoryAllocated -= lElementsCont * m_lSizeofNVTemporalLink;

              lElementsCont = iterNode->second->GetCurrAdditionalChainElementsCount();
              m_bBufferManager->Submit(iterMap->first, iterNode->second->SubmitAdditionalChains(), lElementsCont);

              lElementsCont = iterNode->second->GetCurrMapAdditionalNodesToChainsElementsCount();
              m_bBufferManager->Submit(iterMap->first, iterNode->second->SubmitMapAdditionalNodesToChains(), lElementsCont);

              lElementsCont = iterNode->second->GetCurrMetadataSize();
              m_bBufferManager->Submit(iterMap->first, iterNode->second->SubmitMetadata(), lElementsCont);


          }
      }


      StoreGraphInfo();

      auto stat = m_bBufferManager->HeapUsage();
      m_log_root.info("NVM heap report in SoC("+std::to_string(std::get<0>(stat))+" - heap_size:"+
                                               std::to_string(std::get<1>(stat))+" allocated:"+std::to_string(std::get<2>(stat))+
                                                  " used:"+std::to_string(std::get<3>(stat)));
      DebugInfo::Instance()->ResetCounters();
  }


    void StoreGraphInfo()
    {
        // Create the array of nodes
        //
        std::unique_ptr<tvg_vertex_id[]> data = std::make_unique<tvg_vertex_id[]>(m_mapIDNode.size());
        std::map<tvg_vertex_id, std::unique_ptr<MyVertex>>::iterator iter = m_mapIDNode.begin();
        for (int i = 0 ; iter != m_mapIDNode.end() ; iter++, i++)
        {
            data[i] = iter->first;
        }

       //  m_bBufferManager->Submit(m_lSavedID, std::move(data), m_mapIDNode.size());
        //
        m_bBufferManager->SubmitGenericVector<tvg_vertex_id>(m_strGraphData,  std::move(data), m_mapIDNode.size());



    }





    /*
    void ReleaseMemory()
    {
        long lVertexToReset = GetVertexToReset();
        ResetNode(lVertexToReset);
    }

    long GetVertexToReset()
    {


    }
*/




    BufferManager<TargetNVM>* GetBufferManager()
    {
        return m_bBufferManager;
    }


    ~GraphMemoryManager()
    {
        /*
        std::map<tvg_vertex_id, VertexMemoryData *>::iterator iter = m_mapIDVertexMemory.begin();
        for ( ; iter != m_mapIDVertexMemory.end() ; iter++)
        {
            delete iter->second;
        }
         */
//        m_mapIDVertexMemory.clear();
    }

};


#endif //TVG4TM_GRAPHMEMORYMANAGER_H
