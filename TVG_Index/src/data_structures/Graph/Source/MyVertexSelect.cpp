//
// Created by norig on 12/8/15.
//



#include "../../../infrastructure/TargetNVM.h"


tvg_index MyVertexSelect:: m_iOptimalAdjacentNVVertex = 150;
tvg_index MyVertexSelect::m_lSerialSearchMinSize = 150;
tvg_short_counter MyVertexSelect::m_lCurrMetadataSize = tvg_index(5);

bool MyVertexSelect::m_bInterpolationSearch = false;



MyVertexSelect::MyVertexSelect(tvg_vertex_id iID) : m_lID(iID),_log_root(log4cpp::Category::getRoot())
{
    m_bLoaded = false;
    LoadMetadata();

}




//std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> MyVertexSelect::LoadIntervalNeighbors_from_db(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch,std::unique_ptr<SendUtils<> > sender) const
//{
//        std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> vOutput = std::make_unique<std::vector<std::unique_ptr<NeighborMetaData>>>();
//
//        bool isVertica = false;
//        isVertica = Configuration::Instance()->GetStringItem("database.source_type").find("vertica") != std::string::npos;
//        auto source = std::unique_ptr<DBSource>();
//        if(isVertica)
//        {
//            source = std::move(std::unique_ptr<DBSource>(new DBVerticaSource()));
//        }
//        else
//        {
//            source = std::move(std::unique_ptr<DBSource>(new DBSQLiteSource()));
//        }
//
//        source->Init();
//        auto neighbors_in_db  = source->GetNeighbors(m_lID,tStartEpoch,tEndEpoch);
//        for(auto& nbr : neighbors_in_db)
//        {
//            std::unique_ptr<NeighborMetaData> newValue = std::make_unique<NeighborMetaData>(nbr,-1, 1);
//            vOutput->push_back(std::move(newValue));
//        }
//        source->Finalize();
//        return std::move(vOutput);
//}


std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> MyVertexSelect::LoadIntervalNeighbors_NVRAM(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch, const std::map<tvg_vertex_id, int>& vExcludeNodes)
{
    // The output
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> vOutput;

    std::unique_ptr<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>> hID2Freq;


    // search for the first value
    //
    tvg_index lIndex = SearchPoint_NVRAM(m_lID, tEndEpoch, 0, m_lVertecesCurrCompletedSize - tvg_index(1));



    std::unique_ptr<AdjacentNVVertex[]> nvData;
    if (lIndex.IsValid())
    {
        std::map<tvg_vertex_id, std::pair<tvg_counter, tvg_time>>::iterator iter;
        tvg_vertex_id lID;


        hID2Freq = std::make_unique<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>>();

        tvg_index l = lIndex;

        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, l, 1)); // Optimization latter, no need to recall it in case the last value is correct. .
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::LoadIntervalNeighbors_NVRAM() ERROR: could not load AdjacentNVVertex data of node " + m_lID.GetString() + ", index = " + l.GetString());
            return std::move(vOutput);//drgr try catch right
        }


        while ((nvData != nullptr) && (nvData[0].GetEpoch() >= tStartEpoch))
        {

            lID = nvData[0].GetID();


            // Do not add the "vExcludeNodes"
            //
            std::map<tvg_vertex_id, int>::const_iterator iterExcludeNode = vExcludeNodes.find(lID);
            if (iterExcludeNode == vExcludeNodes.end())
            {
                std::unique_ptr<NeighborMetaData> pNewNeighborMetaData = std::make_unique<NeighborMetaData>(lID, nvData[0].GetEpoch(), 1);
                (*hID2Freq)[lID] = std::move(pNewNeighborMetaData);
            }


            l = LoadNext_NVRAM(l, tEndEpoch);
            if (!l.IsValid())
                break;


            try
            {
                nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, l, 1)); // Optimization latter, no need to recall it in case the last value is correct. .
            }
            catch (...)
            {
                _log_root.error("MyVertexSelect::LoadIntervalNeighbors_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get(),l.Get());
                return std::move(vOutput);//drgr try catch right
            }
        }

    }


    // Constract the output as a vector
    //
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




std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> MyVertexSelect::LoadIntervalNeighbors_NVRAM(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch)
{
    // The output
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> vOutput;

    std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>> hID2Freq;

    tvg_index lIndex;
    // Binary search for the first value
    //



    /*
    try
    {
        nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, m_lVertecesCurrCompletedSize - tvg_index(1), 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::LoadIntervalNeighbors_NVRAM() ERROR: could not load AdjacentNVVertex data of node " + m_lID.GetString() + ", index = " + (m_lVertecesCurrCompletedSize - tvg_index(1)).GetString());
        return std::move(vOutput);
    }
*/

        lIndex = SearchPoint_NVRAM(m_lID, tEndEpoch, 0, m_lVertecesCurrCompletedSize - tvg_index(1));


    if (lIndex.IsValid())
    {
        std::map<tvg_vertex_id, std::pair<tvg_counter, tvg_time>>::iterator iter;
        tvg_vertex_id lID;


//        hID2Freq = std::make_unique<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>>();


        tvg_index l = lIndex;


        std::unique_ptr<AdjacentNVVertex[]> nvData;
        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, l, 1)); // Optimization latter, no need to recall it in case the last value is correct. .
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::LoadIntervalNeighbors_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get(),l.Get());

            return std::move(vOutput);//drgr try catch right
        }


        while ((nvData != nullptr) && (nvData[0].GetEpoch() >= tStartEpoch))
        {
            if (nvData[0].GetEpoch() <= tEndEpoch)
            {

                lID = nvData[0].GetID();

                std::unique_ptr<NeighborMetaData> pNewNeighborMetaData = std::make_unique<NeighborMetaData>(lID, nvData[0].GetEpoch(), 1);
                hID2Freq[lID] = std::move(pNewNeighborMetaData);
            }


            l = LoadNext_NVRAM(l, tEndEpoch);
            if (!l.IsValid())
                break;

            try
            {
                nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, l, 1)); // Optimization latter, no need to recall it in case the last value is correct. .
            }
            catch (...)
            {
                _log_root.error("MyVertexSelect::LoadIntervalNeighbors_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get(),l.Get());

                return std::move(vOutput);//drgr try catch right
            }
        }

    }


    // Constract the output as a vector
    //
    if (!hID2Freq.empty())
    {
        vOutput = std::make_unique<std::vector<std::unique_ptr<NeighborMetaData>>>(hID2Freq.size());

        std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterOut = hID2Freq.begin();
        for (int i = 0; iterOut != hID2Freq.end(); i++, iterOut++)
        {
            (*vOutput)[i] = std::move(iterOut->second);
        }
    }

    return std::move(vOutput);
}




/*

std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> MyVertexSelect::LoadIntervalNeighbors_NVRAM_Efficient(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch, std::unique_ptr<SendUtils<> > sender)
{
    // The output
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> vOutput;

    std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>> hID2Freq;

    tvg_index lIndex;
    // Binary search for the first value
    //

    std::unique_ptr<AdjacentNVVertex[]> nvData;
    try
    {
        nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, m_lVertecesCurrCompletedSize - tvg_index(1), 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::LoadIntervalNeighbors_NVRAM() ERROR: could not load AdjacentNVVertex data of node " + m_lID.GetString() + ", index = " + (m_lVertecesCurrCompletedSize - tvg_index(1)).GetString());
        return std::move(vOutput);
    }



    lIndex = InterpolationSearchPoint_NVRAM(m_lID, tEndEpoch, 0, m_lVertecesCurrCompletedSize - tvg_index(1));


    if (lIndex.IsValid())
    {
        std::map<tvg_vertex_id, std::pair<tvg_counter, tvg_time>>::iterator iter;
        tvg_vertex_id lID;


//        hID2Freq = std::make_unique<std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>>();


        tvg_index l = lIndex;

        do
        {
            try
            {
                nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, l, 1)); // Optimization latter, no need to recall it in case the last value is correct. .
            }
            catch (...)
            {
                _log_root.error("MyVertexSelect::LoadIntervalNeighbors_NVRAM() ERROR: could not load AdjacentNVVertex data of node " + m_lID.GetString() + ", index = " + l.GetString());
                return std::move(vOutput);//drgr try catch right
            }


            lID = nvData[0].GetID();

            std::unique_ptr<NeighborMetaData> pNewNeighborMetaData = std::make_unique<NeighborMetaData>(lID, nvData[0].GetEpoch(), 1);
            hID2Freq[lID] = std::move(pNewNeighborMetaData);


            l = LoadNext_NVRAM(l, tEndEpoch);
        }
        while ((nvData != nullptr) && (l.IsValid()) && (nvData[0].GetEpoch() >= tStartEpoch));
    }


    // Constract the output as a vector
    //
    if (!hID2Freq.empty())
    {
        vOutput = std::make_unique<std::vector<std::unique_ptr<NeighborMetaData>>>(hID2Freq.size());

        std::map<tvg_vertex_id, std::unique_ptr<NeighborMetaData>>::iterator iterOut = hID2Freq.begin();
        for (int i = 0; iterOut != hID2Freq.end(); i++, iterOut++)
        {
            (*vOutput)[i] = std::move(iterOut->second);
        }
    }

    return std::move(vOutput);
}
*/

tvg_index MyVertexSelect::LoadNext_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch)
{
    std::unique_ptr<NVChainStructure[]> nvData;
    try
    {
        nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<NVChainStructure>(m_lID, lCurr, 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::LoadNext_NVRAM() ERROR: could not load NVChainStructure data of node %llu, index = %llu",m_lID.Get(),lCurr.Get());

        return tvg_index(-1);
    }



    return nvData[0].GetSuccessor_NVRAM(lCurr, tEndEpoch, *this);
}

void MyVertexSelect::LoadMapAdditionalNodesToChains()
{
    if (!m_lAdditionalChainCurrLength.IsValid() || (m_lAdditionalChainCurrLength < tvg_index(1)) ||
        !m_lAdditionalChainMatchingCurrLength.IsValid() || (m_lAdditionalChainMatchingCurrLength < tvg_index(1))    )
        return;


//    m_aAdditionalChains = std::move(BufferManager<TargetNVM>::Instance()->Get<NVTemporalLink>(m_lID, 0, m_lAdditionalChainCurrLength));


    std::unique_ptr<NVLinkPairInterval[]> nvData;
    try
    {
        nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<NVLinkPairInterval>(m_lID, 0, m_lAdditionalChainMatchingCurrLength));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::LoadMapAdditionalNodesToChains() ERROR: could not load NVLinkPairInterval data of node %llu, index = %llu",m_lID.Get(),m_lAdditionalChainMatchingCurrLength.Get());
        return;
    }



    if (nvData == nullptr)
    {
        return;
    }

//    m_mapAdditionalNodesToChains = std::make_unique<std::map<NVLink, NVIntervalLink>>();
    for (int i = 0 ; i <  m_lAdditionalChainMatchingCurrLength ; i++)
    {
        m_mapAdditionalNodesToChains[nvData[i].GetFirstLink()] = nvData[i].GetSecondLink();
    }
}


void MyVertexSelect::LoadMetadata()
{
    if (!m_bLoaded)
    {
        std::unique_ptr<tvg_index[]> sermetadata;

        try
        {
            sermetadata = std::move(BufferManager<TargetNVM>::Instance()->Get<tvg_index>(m_lID, 0, GetCurrMetadataSize()));
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::LoadMetadata() ERROR: could not load tvg_index data of node %llu ",m_lID.Get());
            return;
        }


        m_lVertecesCurrCompletedSize = sermetadata[0];
        m_lVertecesRemovedSize = sermetadata[1];
//        m_lSavedChainRemovedSize = sermetadata[2];
//        m_lSavedChainCurrSize = sermetadata[3];
        m_lAdditionalChainRemovedLength = sermetadata[2];
        m_lAdditionalChainCurrLength = sermetadata[3];
        m_lAdditionalChainMatchingCurrLength = sermetadata[4];


        if (m_lAdditionalChainCurrLength.IsValid() && (m_lAdditionalChainCurrLength > tvg_index(0)) &&
                m_lAdditionalChainMatchingCurrLength.IsValid() && (m_lAdditionalChainMatchingCurrLength > tvg_index(0)))
            LoadMapAdditionalNodesToChains();

        m_bLoaded = true;
    }


}



std::unique_ptr<std::list<tvg_time>> MyVertexSelect::GetLastConnectionAppearence(const tvg_vertex_id& lInputID, const tvg_time& tStartInterval, const tvg_time& tEndInterval)
{
    // The output
    //
    std::unique_ptr<std::list<tvg_time>> listOfTimes = std::make_unique<std::list<tvg_time>>();



    tvg_index lIndex;
    // 1. Binary search for the first value of the period = lIndex
    //


 /*   try
    {
        nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, m_lVertecesCurrCompletedSize - tvg_index(1), 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::GetLastConnectionAppearence() ERROR: could not load AdjacentNVVertex data of node " + m_lID.GetString() + ", index = " + (m_lVertecesCurrCompletedSize - tvg_index(1)).GetString());
        return std::move(listOfTimes);
    }
*/

    lIndex = SearchPoint_NVRAM(m_lID, tEndInterval + 1, 0, m_lVertecesCurrCompletedSize - tvg_index(1));



    if (!lIndex.IsValid())
        return std::move(listOfTimes);



/////////////////////////////////////////////////////////////////////////////////////////////

    tvg_index lLastAppearence;
    // 2. skip backwards, until reaching the element = lLastAppearence
    //
    std::map<tvg_vertex_id, std::pair<tvg_counter, tvg_time>>::iterator iter;
    tvg_vertex_id lID;


    std::unique_ptr<AdjacentNVVertex[]> nvData;
    tvg_index l = lIndex;
    do
    {

        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, l, 1)); // Optimization latter, no need to recall it in case the last value is correct. .
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::GetLastConnectionAppearence() ERROR: could not load AdjacentNVVertex data of node %llu, index= %llu" ,m_lID.Get() ,l.Get());
            return std::move(listOfTimes);//drgr try catch right
        }


        lID = nvData[0].GetID();

        if (lInputID == lID)
        {
            lLastAppearence = l;
            break;
        }

        l = LoadNext_NVRAM(l, tEndInterval);
    }
    while ((nvData != nullptr) && (l.IsValid()) && (nvData[0].GetEpoch() >= tStartInterval));


    if (!lLastAppearence.IsValid())
        return std::move(listOfTimes);



/////////////////////////////////////////////////////////////////////////////////////////////




    // 3. Jump on links starting at lLastAppearence
    //
    while ((nvData != nullptr) && (lLastAppearence.IsValid()) && nvData[0].GetEpoch() >= tStartInterval)
    {
        listOfTimes->push_front(nvData[0].GetEpoch());


        lLastAppearence = nvData[0].GetSameNodePrev().GetIndex();


        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, lLastAppearence, 1));
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::GetLastConnectionAppearence() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get(),lLastAppearence.Get());
            return std::move(listOfTimes);//drgr try catch right
        }
    }


    return std::move(listOfTimes);
}

std::unique_ptr<std::list<tvg_time>> MyVertexSelect::GetLastConnectionAppearence_Efficient(const tvg_vertex_id& lInputID, const tvg_time& tStartInterval, const tvg_time& tEndInterval)
{
    // The output
    //
    std::unique_ptr<std::list<tvg_time>> listOfTimes = std::make_unique<std::list<tvg_time>>();



    tvg_index lIndex;
    // 1. Interpolation search for the first value of the period = lIndex
    //


    /*   try
       {
           nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, m_lVertecesCurrCompletedSize - tvg_index(1), 1));
       }
       catch (...)
       {
           _log_root.error("MyVertexSelect::GetLastConnectionAppearence() ERROR: could not load AdjacentNVVertex data of node " + m_lID.GetString() + ", index = " + (m_lVertecesCurrCompletedSize - tvg_index(1)).GetString());
           return std::move(listOfTimes);
       }
   */

    lIndex = SearchPoint_NVRAM(m_lID, tEndInterval, 0, m_lVertecesCurrCompletedSize - tvg_index(1));



    if (!lIndex.IsValid())
        return std::move(listOfTimes);



/////////////////////////////////////////////////////////////////////////////////////////////

    tvg_index lLastAppearence;
    // 2. skip backwards, until reaching the element = lLastAppearence
    //
    std::map<tvg_vertex_id, std::pair<tvg_counter, tvg_time>>::iterator iter;
    tvg_vertex_id lID;


    std::unique_ptr<AdjacentNVVertex[]> nvData;
    tvg_index l = lIndex;
    do
    {

        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, l, 1)); // Optimization latter, no need to recall it in case the last value is correct. .
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::GetLastConnectionAppearence() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get(),l.Get());

            return std::move(listOfTimes);//drgr try catch right
        }


        lID = nvData[0].GetID();

        if (lInputID == lID)
        {
            lLastAppearence = l;
            break;
        }

        l = LoadNext_NVRAM(l, tEndInterval);
    }
    while ((nvData != nullptr) && (l.IsValid()) && (nvData[0].GetEpoch() >= tStartInterval));


    if (!lLastAppearence.IsValid())
        return std::move(listOfTimes);



/////////////////////////////////////////////////////////////////////////////////////////////




    // 3. Jump on links starting at lLastAppearence
    //
    while ((nvData != nullptr) && (lLastAppearence.IsValid()) && nvData[0].GetEpoch() >= tStartInterval)
    {
        listOfTimes->push_front(nvData[0].GetEpoch());


        lLastAppearence = nvData[0].GetSameNodePrev().GetIndex();


        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, lLastAppearence, 1));
        }
        catch (...)
        {
             _log_root.error("MyVertexSelect::GetLastConnectionAppearence() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get(),lLastAppearence.Get());
            return std::move(listOfTimes);//drgr try catch right
        }
    }


    return std::move(listOfTimes);
}


tvg_index MyVertexSelect::GetSuccessorInAdditionalLinks_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch) // probably need to perform a binary search - later
{
    tvg_index indexResult;
    SameNodeNVLink nvLink(lCurr);

    std::map<SameNodeNVLink, NVIntervalLink>::iterator iter = m_mapAdditionalNodesToChains.find(nvLink);
    if (iter == m_mapAdditionalNodesToChains.end())
    {
        _log_root.error("NVRepository::GetSuccessorInAdditionalLinks(): cannot find element on index = %llu",lCurr.Get());
        return indexResult;
    }


    std::unique_ptr<SameNodeNVTemporalLink[]> nvData;
    try
    {
        nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<SameNodeNVTemporalLink>(m_lID, iter->second.GetIndex(), iter->second.GetSize()));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::GetSuccessorInAdditionalLinks_NVRAM() ERROR: could not load NVTemporalLink data of node %llu, index = %llu, length = %llu",m_lID.Get(),iter->second.GetIndex().Get(),iter->second.GetSize().Get());

        return tvg_index(-1);
    }



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



tvg_index MyVertexSelect::SearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast)
{
    if (m_bInterpolationSearch)
        return InterpolationSearchPoint_NVRAM(lID, tEpoch, lFirst, lLast);
    else
        return BinarySearchPoint_NVRAM(lID, tEpoch, lFirst, lLast);
}




tvg_index MyVertexSelect::BinarySearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast)
{
//    high_resolution_clock::time_point tBinaryStart;
//    if (PerformanceMeter::Instance()->DoPerformMeterData())
//    {
//        tBinaryStart = std::chrono::high_resolution_clock::now();
//    }



//    if ((!lFirst.IsValid()) || (!lLast.IsValid()) || (lFirst > lLast))
//    {
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tBinaryStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddBinaryExecution(tDuration);
//        }
//        return -1;
//    }


    std::unique_ptr<AdjacentNVVertex[]> nvData;


    // For preventing out of time
    //
    std::unique_ptr<AdjacentNVVertex[]> nvFirstValue;
    try
    {
        nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, lFirst, 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::BinarySearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",lID.Get() , lFirst.Get());
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tBinaryStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddBinaryExecution(tDuration);
//        }

        return tvg_index(-1);
    }
    if (tEpoch < nvData[0].GetEpoch())
    {
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tBinaryStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddBinaryExecution(tDuration);
//        }

        return tvg_index(-1);
    }





    try
    {
        nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, lLast, 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::BinarySearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",lID.Get(), lLast.Get());
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tBinaryStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddBinaryExecution(tDuration);
//        }

        return tvg_index(-1);
    }



    tvg_index tResul = BinarySearchPoint_NVRAM(lID, tEpoch, lFirst, lLast, std::move(nvData), tvg_index(0));
//    if (PerformanceMeter::Instance()->DoPerformMeterData())
//    {
//        auto tEnd = std::chrono::high_resolution_clock::now() - tBinaryStart;
//        auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//        PerformanceMeter::Instance()->AddBinaryExecution(tDuration);
//    }

    return tResul;
}



tvg_index MyVertexSelect::BinarySearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast, std::unique_ptr<AdjacentNVVertex[]> data, tvg_index lIndex)
{
    if ((!lFirst.IsValid()) || (!lLast.IsValid()) || (lFirst > lLast))
        return -1;

    if (tEpoch >= data[lIndex].GetEpoch())
    {
        return lLast;
    }



        tvg_index lMid = (lFirst + lLast)/tvg_index(2);

        int iShift = 0;
        std::unique_ptr<AdjacentNVVertex[]> nvData;
        try
        {
            if (lMid.Get() > 0)
            {
                iShift = 1;
                nvData = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid.Get() - 1, 2));
            }
            else
                nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid, 1));
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::BinarySearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",lID.Get(),lMid.Get());
            return tvg_index(-1);
        }


        if (lMid == lFirst)
        {
            if (lMid == lLast)
            {
                if (lMid > tvg_index(0) && (tEpoch < nvData[iShift].GetEpoch()))
                    return (lMid - tvg_index(1));
                else
                    return lMid;
            }

            else
            {
                if (tEpoch < nvData[iShift].GetEpoch())
                    return lMid-tvg_index(1); // -1
                else
                    return lMid;
            }
        }
        else
        {
            if (tEpoch == nvData[iShift].GetEpoch())
                return BinarySearchPoint_NVRAM(lID, tEpoch, lMid, lLast, std::move(data), lIndex);
            else
            {
                if (tEpoch < nvData[iShift].GetEpoch())
                {
                    /*
                    std::unique_ptr<AdjacentNVVertex[]> nvData;
                    try
                    {
                        nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid - tvg_index(1), 1));
                    }
                    catch (...)
                    {
                        _log_root.error("MyVertexSelect::BinarySearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node " + lID.GetString() + ", index = " + (lMid - tvg_index(1)).GetString());
                        return tvg_index(-1);
                    }
*/

                    return BinarySearchPoint_NVRAM(lID, tEpoch, lFirst, lMid - tvg_index(1), std::move(nvData), tvg_index(0));
                }
                else
                    return BinarySearchPoint_NVRAM(lID, tEpoch, lMid, lLast, std::move(data), lIndex);
            }
        }
}




tvg_index MyVertexSelect::InterpolationSearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast)
{
//    high_resolution_clock::time_point tInterpolationStart;
//    if (PerformanceMeter::Instance()->DoPerformMeterData())
//    {
//        tInterpolationStart = std::chrono::high_resolution_clock::now();
//    }
//
//
//    if ((!lFirst.IsValid()) || (!lLast.IsValid()) || (lFirst > lLast))
//    {
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tInterpolationStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddInterpolationExecution(tDuration);
//        }
//        return -1;
//    }


    // First Value

    std::unique_ptr<AdjacentNVVertex[]> nvFirstValue;
    try
    {
        nvFirstValue = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, lFirst, 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::InterpolationSearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",lID.Get(), lFirst.Get());
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tInterpolationStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddInterpolationExecution(tDuration);
//        }
        return tvg_index(-1);
    }



    // Last Value
    //
    std::unique_ptr<AdjacentNVVertex[]> nvLastValue;
    try
    {
        nvLastValue = std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, lLast, 1));
    }
    catch (...)
    {
        _log_root.error("MyVertexSelect::InterpolationSearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",lID.Get(),lLast.Get());
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tInterpolationStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddInterpolationExecution(tDuration);
//        }
        return tvg_index(-1);
    }


    tvg_index tResul = InterpolationSearchPoint_NVRAM(lID, tEpoch, lFirst, std::move(nvFirstValue), tvg_index(0), lLast, std::move(nvLastValue), tvg_index(0));
//    if (PerformanceMeter::Instance()->DoPerformMeterData())
//    {
//        auto tEnd = std::chrono::high_resolution_clock::now() - tInterpolationStart;
//        auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//        PerformanceMeter::Instance()->AddInterpolationExecution(tDuration);
//    }

    return tResul;
}



tvg_index MyVertexSelect::SerialSearchPointLR_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast)
{
    tvg_short_counter tOptimalBatchSize = GetOptimalAdjacentNVVertex();


    do
    {
        if (tOptimalBatchSize > (lLast - lFirst + 1))
            tOptimalBatchSize = (lLast - lFirst + 1);

        if (tOptimalBatchSize == 0)
            break;

        std::unique_ptr<AdjacentNVVertex[]> nvData;

        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lFirst, tOptimalBatchSize));
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::GetNextConnections() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get(), lFirst.Get());
            return tvg_index(-1);
        }



        // if the last value is in the interval
        //
        if (nvData[tOptimalBatchSize - 1].GetEpoch() >= tEpoch)
        {
            if (nvData[0].GetEpoch() > tEpoch)
            {
                return lFirst - tvg_index(1);
            }

            for (int i = 0 ; i < tOptimalBatchSize - 1 ; i++)
            {
                if ((nvData[i].GetEpoch() <= tEpoch) && (nvData[i+1].GetEpoch() > tEpoch))
                    return lFirst + tvg_index(i + 1);
            }

        }


        lFirst += tOptimalBatchSize;
    }
    while(lFirst < lLast);



    return lLast;

}


tvg_index MyVertexSelect::SerialSearchPointRL_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch, tvg_index lFirst, tvg_index lLast)
{
    tvg_short_counter tOptimalBatchSize = GetOptimalAdjacentNVVertex();

    tvg_short_counter tCurrSize;

    do
    {
        if (tOptimalBatchSize > (lLast - lFirst + 1))
            tOptimalBatchSize = (lLast - lFirst + 1);

        if (tOptimalBatchSize == 0)
            break;

        std::unique_ptr<AdjacentNVVertex[]> nvData;


        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lLast - tOptimalBatchSize + 1, tOptimalBatchSize));
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::GetNextConnections() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get(), lFirst.Get());
            return tvg_index(-1);
        }



        // if the last value is in the interval
        //
        if (nvData[0].GetEpoch() <= tEpoch)
        {
            if (nvData[tOptimalBatchSize - 1].GetEpoch() < tEpoch)
            {
                return lLast;
            }

            for (int i = 0 ; i < tOptimalBatchSize - 1 ; i++)
            {
                if ((nvData[i].GetEpoch() <= tEpoch) && (nvData[i+1].GetEpoch() > tEpoch))
                    return (lLast - tOptimalBatchSize + tvg_index(i + 2));
            }

        }


        lLast -= tOptimalBatchSize;
    }
    while(lFirst < lLast);



    return lLast;

}


tvg_index MyVertexSelect::InterpolationSearchPoint_NVRAM(const tvg_vertex_id& lID, const tvg_time& tEpoch,
                                                         tvg_index lFirst, std::unique_ptr<AdjacentNVVertex[]> nvFirstData, tvg_index lFirstDataIndex,
                                                         tvg_index lLast, std::unique_ptr<AdjacentNVVertex[]>  nvLastData, tvg_index lLastDataIndex)
{
    if ((!lFirst.IsValid()) || (!lLast.IsValid()) || (lFirst > lLast))
        return -1;

    if (tEpoch < nvFirstData[lFirstDataIndex].GetEpoch())
    {
        return (lFirst.Get() - 1);
    }

    if (tEpoch >= nvLastData[lLastDataIndex].GetEpoch())
    {
        return lLast;
    }



    double dMidTime = double(tEpoch - nvFirstData[lFirstDataIndex].GetEpoch())/std::max(1.0, double(nvLastData[lLastDataIndex].GetEpoch() - nvFirstData[lFirstDataIndex].GetEpoch()));
    tvg_index lMid = lFirst.Get() + (lLast - lFirst + 1) * dMidTime;

    if (lMid > lLast)
        lMid = lLast;


    if ((lMid - lFirst) < GetSerialSearchMinSize())
    {

//        high_resolution_clock::time_point tSerialStart;
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            tSerialStart = std::chrono::high_resolution_clock::now();
//        }

        tvg_index tRes = SerialSearchPointLR_NVRAM(lID, tEpoch, lFirst, lLast);

//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tSerialStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddSerialExecution(tDuration);
//        }


        //       std::cout<<"Serial search LR"<<std::endl;
        return tRes;
    }

    if ((lLast - lMid) < GetSerialSearchMinSize())
    {
//        high_resolution_clock::time_point tSerialStart;
//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            tSerialStart = std::chrono::high_resolution_clock::now();
//        }

        tvg_index tRes =  SerialSearchPointRL_NVRAM(lID, tEpoch, lFirst, lLast);

//        if (PerformanceMeter::Instance()->DoPerformMeterData())
//        {
//            auto tEnd = std::chrono::high_resolution_clock::now() - tSerialStart;
//            auto tDuration = std::chrono::duration_cast<milliseconds>(tEnd).count();
//
//            PerformanceMeter::Instance()->AddSerialExecution(tDuration);
//        }

        return tRes;
    }



    std::unique_ptr<AdjacentNVVertex[]> nvMidData;
        int iShift = 0;
        try
        {
            if (lMid.Get() > 0)
            {
                iShift = 1;
                nvMidData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid.Get() - tvg_index(1), 2));
            }
            else
            {
                nvMidData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid, 1));
            }
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::InterpolationSearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu index = %llu",lID.Get(),lMid.Get());
            return tvg_index(-1);
        }


        if (lMid == lFirst)
        {
            if (lMid == lLast)
            {
                if (lMid > tvg_index(0) && (tEpoch < nvMidData[iShift].GetEpoch()))
                    return (lMid - tvg_index(1));
                else
                    return lMid;
            }

            else
            {
                if (tEpoch < nvMidData[iShift].GetEpoch())
                    return lMid-tvg_index(1); // -1
                else
                {
                    if (tEpoch == nvMidData[iShift].GetEpoch())
                        return lMid;
                    else
                    {
                        try
                        {
                            nvMidData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid + tvg_index(1), 1));
                        }

                        catch (...)
                        {
                            _log_root.error("MyVertexSelect::InterpolationSearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",lID.Get(),(lMid.Get() + tvg_index(1)));
                            return tvg_index(-1);
                        }

                        return InterpolationSearchPoint_NVRAM(lID, tEpoch, lMid + tvg_index(1), std::move(nvMidData), tvg_index(0), lLast, std::move(nvLastData), lLastDataIndex);
                    }
                }
            }
        }
        else
        {
            if (tEpoch == nvMidData[iShift].GetEpoch())
                return InterpolationSearchPoint_NVRAM(lID, tEpoch, lMid, std::move(nvMidData), tvg_index(iShift), lLast, std::move(nvLastData), lLastDataIndex);
            else
            {
                if (tEpoch < nvMidData[iShift].GetEpoch())
                {
                    /*
                    std::unique_ptr<AdjacentNVVertex[]> nvMidData;
                    try
                    {
                        nvMidData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(lID, lMid - tvg_index(1), 1));
                    }
                    catch (...)
                    {
                        _log_root.error("MyVertexSelect::InterpolationSearchPoint_NVRAM() ERROR: could not load AdjacentNVVertex data of node " + lID.GetString() + ", index = " + (lMid - tvg_index(1)).GetString());
                        return tvg_index(-1);
                    }
*/

                    return InterpolationSearchPoint_NVRAM(lID, tEpoch, lFirst, std::move(nvFirstData), lFirstDataIndex, lMid - tvg_index(1), std::move(nvMidData), tvg_index(0));
                }
                else
                    return InterpolationSearchPoint_NVRAM(lID, tEpoch, lMid, std::move(nvMidData), iShift, lLast, std::move(nvLastData), lLastDataIndex);
            }
        }
}


std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> MyVertexSelect::GetNextConnections(const std::unique_ptr<std::vector<tvg_vertex_id>>& vInputExcludeNodes, tvg_time tTime, tvg_time tMaxWindowShift, bool bAddition)
{
    std::unique_ptr<std::vector<std::unique_ptr<MyEdgeTemporalAction>>> vOutput = std::make_unique<std::vector<std::unique_ptr<MyEdgeTemporalAction>>>();

    tvg_index lStatIndex = SearchPoint_NVRAM(m_lID, tTime, 0, m_lVertecesCurrCompletedSize - tvg_index(1));
    if (!lStatIndex.IsValid())
        lStatIndex = tvg_index(0);


    tvg_short_counter tOptimalBatchSize = GetOptimalAdjacentNVVertex();


    tvg_time tEndTime = tTime + tMaxWindowShift;
    tvg_time tCurrTime = tTime;
    tvg_vertex_id lCurrID;

    do
    {
        if (tOptimalBatchSize > (m_lVertecesCurrCompletedSize - lStatIndex))
            tOptimalBatchSize = (m_lVertecesCurrCompletedSize - lStatIndex);

        if (tOptimalBatchSize == 0)
            break;

        std::unique_ptr<AdjacentNVVertex[]> nvData;

        try
        {
            nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<AdjacentNVVertex>(m_lID, lStatIndex, tOptimalBatchSize));
        }
        catch (...)
        {
            _log_root.error("MyVertexSelect::GetNextConnections() ERROR: could not load AdjacentNVVertex data of node %llu, index = %llu",m_lID.Get() ,lStatIndex.Get());
            return std::move(vOutput);
        }



        // if the last value is in the interval
        //
        if (nvData[tOptimalBatchSize - 1].GetEpoch() >= tTime)
        {
            for (int i = 0 ; i < tOptimalBatchSize ; i++)
            {
                tCurrTime = nvData[i].GetEpoch();
                if (tEndTime < tCurrTime)
                    return std::move(vOutput);

                lCurrID = nvData[i].GetID();
                //   if (vInputExcludeNodes->find(lCurrID) != vInputExcludeNodes->end())
                if (std::find(vInputExcludeNodes->begin(), vInputExcludeNodes->end(), lCurrID) != vInputExcludeNodes->end())
                    continue;

                std::unique_ptr<MyEdgeTemporalAction> pNewEdgeTemporalAction;
                if (nvData[i].IsSource())
                    pNewEdgeTemporalAction = std::make_unique<MyEdgeTemporalAction>(m_lID, lCurrID, tCurrTime, bAddition);
                else
                    pNewEdgeTemporalAction = std::make_unique<MyEdgeTemporalAction>(lCurrID, m_lID, tCurrTime, bAddition);

                vOutput->push_back(std::move(pNewEdgeTemporalAction));
            }
        }



        lStatIndex += tOptimalBatchSize;
    }
    while((tEndTime > tCurrTime) && (lStatIndex < m_lVertecesCurrCompletedSize));



    return std::move(vOutput);
}
