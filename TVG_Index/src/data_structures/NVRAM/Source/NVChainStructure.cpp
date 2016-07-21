//
// Created by norig on 6/24/15.
//




#include "../Header/NVChainStructure.h"
#include "../../General/Header/Stats.h"
#include "../../../infrastructure/TargetNVM.h"


///////////////////////////////////////////////////////////////////
//
NVChainStructure::NVChainStructure()
{


//    m_aChain = new NVChainElem[Configuration::Instance()->GetIntegerItem("data-structure.chain-structure-size")];//SAGI load from config file
    m_iRealSize = tvg_index(0);
//    m_iAdditionalLinksSize = 0;
}

//NVChainStructure::NVChainStructure(const NVChainStructure& elem)
//{
//    *this = elem;
//}
NVChainStructure::NVChainStructure(const NVChainStructure & that):m_iRealSize(that.m_iRealSize)
{
    for(int i=0;i<m_iRealSize;i++)
        m_aChain[i] = that.m_aChain[i];
}
const NVChainStructure &NVChainStructure::operator=(const NVChainStructure& elem)
{
//    m_aChain = new NVChainElem[Configuration::Instance()->GetIntegerItem("data-structure.chain-structure-size")]; //SAGI

    m_iRealSize = elem.m_iRealSize;

    for (tvg_short_counter i = 0 ; i < m_iRealSize ; i++)
    {
        m_aChain[i] = elem.m_aChain[i];
    }
    return *this;
}

NVChainStructure::~NVChainStructure()
{
//    if (m_aChain)
//        delete [] m_aChain;
}




/*
tvg_index NVChainStructure::GetSuccessor_NVRAM(const tvg_vertex_id& lID, const tvg_index& lCurr, const tvg_time& tEndEpoch) // meanwhile serial search, probably the best in most cases
{
    std::unique_ptr<NVTemporalLink[]> nvData;
    if (m_iRealSize > m_iMaxChainSize)
    {
        nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<NVTemporalLink>(lID, m_iMaxChainSize * (lCurr - 1), 1));
        if (nvData[0].GetEpoch() < tEndEpoch)
            return GetSuccessorInAdditionalLinks(tEndEpoch);
    }


    // else, the value is in the main chain, just serially check there
    //
    nvData =  std::move(BufferManager<TargetNVM>::Instance()->Get<NVTemporalLink>(lID, m_iMaxChainSize * lCurr, m_iRealSize));

    for (tvg_short_counter i = 0 ; i < m_iRealSize ; i++)
    {
        if (i > 0)
        {
            if (nvData[i].GetEpoch() > tEndEpoch)
            {
                return nvData[i-1].GetIndex();
            }

        }
        else
        {
            if (nvData[0].GetEpoch() > tEndEpoch)
            {
                return -1;
            }

        }

    }


    // if get there, the last one is the result
    //
    return nvData[m_iRealSize-1].GetIndex();
}
*/


tvg_index NVChainStructure::GetSuccessor_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch, MyVertexSelect& pVertex)
{
    if (m_iRealSize > m_iMaxChainSize)
    {
        if (m_aChain[m_iMaxChainSize-1].GetEpoch() < tEndEpoch)
            return pVertex.GetSuccessorInAdditionalLinks_NVRAM(lCurr, tEndEpoch);
    }


    // else, the value is in the main chain, just serially check there
    //
    for (tvg_short_counter i = 0 ; i < m_iRealSize ; i++)
    {
        if (m_aChain[i].GetEpoch() > tEndEpoch)
        {
            if (i > 0)
                return m_aChain[i-1].GetIndex();
            else
                return -1;
        }
    }


    // if get there, the last one is the result
    //
    return m_aChain[m_iRealSize-tvg_index(1)].GetIndex();
}


tvg_index NVChainStructure::GetSuccessor_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch, NVRepository& pRepository)
{
    if (m_iRealSize > m_iMaxChainSize)
    {
        if (m_aChain[m_iMaxChainSize-1].GetEpoch() < tEndEpoch)
            return pRepository.GetSuccessorInAdditionalLinks_NVRAM(lCurr, tEndEpoch);
    }


    // else, the value is in the main chain, just serially check there
    //
    for (tvg_short_counter i = 0 ; i < m_iRealSize ; i++)
    {
        if (m_aChain[i].GetEpoch() > tEndEpoch)
        {
            if (i > 0)
                return m_aChain[i-1].GetIndex();
            else
                return -1;
        }
    }


    // if get there, the last one is the result
    //
    return m_aChain[m_iRealSize-tvg_index(1)].GetIndex();
}



tvg_index NVChainStructure::GetSuccessor(const tvg_index& lCurr, const tvg_time& tEndEpoch, NVRepository& pRepository) // meanwhile serial search, probably the best in most cases
{
    if (m_iRealSize > m_iMaxChainSize)
    {
        if (m_aChain[m_iMaxChainSize-1].GetEpoch() < tEndEpoch)
            return pRepository.GetSuccessorInAdditionalLinks(lCurr, tEndEpoch);
    }


    // else, the value is in the main chain, just serially check there
    //
    for (tvg_short_counter i = 0 ; i < m_iRealSize ; i++)
    {
        if (m_aChain[i].GetEpoch() > tEndEpoch)
        {
            if (i > 0)
                return m_aChain[i-1].GetIndex();
            else
                return -1;
        }
    }


    // if get there, the last one is the result
    //
    return m_aChain[m_iRealSize-tvg_index(1)].GetIndex();
}








void NVChainStructure::SetData(std::unique_ptr<AdjacentDStructure> pCompleteRes, const tvg_vertex_id& lID, NVRepository& pRepository)
{
    if (pCompleteRes == NULL)
        return;

    std::list<std::unique_ptr<SameNodeNVTemporalLink>>& lLinks = pCompleteRes->GetChain();
    m_iRealSize = lLinks.size();


    // set the eq link
    //
    //m_lPrevEq = pCompleteRes->                                        /////////// IGOR

    Stats::Instance()->AddToHistogram(m_iRealSize);

    std::list<std::unique_ptr<SameNodeNVTemporalLink>>::iterator iterLinks = lLinks.begin();
    for (int iCounter = 0; iterLinks != lLinks.end(); iterLinks++, iCounter++)
    {
        if (iCounter >= m_iMaxChainSize)
        {
            log4cpp::Category::getRoot().debug("NVChainStructure::SetData(): the chain size is " + std::to_string(lLinks.size())); // exception

            pRepository.CompleteSetData(lID, pCompleteRes->GetIndex(), lLinks, iCounter, iterLinks);
            break;
        }
        m_aChain[iCounter].SetEpoch((*iterLinks)->GetEpoch());

        // std::unique_ptr<NVLink> pLink = std::make_unique<NVLink>((*iterLinks)->GetVertexID(), (*iterLinks)->GetIndex()); // old version, writes theID of the linked node, instead of THIS node
        m_aChain[iCounter].SetIndex((*iterLinks)->GetIndex());
    }
}




tvg_long_counter NVChainStructure::Submit(std::ofstream& ofstream, unsigned long iIndex)
{
    ofstream<<"---------------------------"<<std::endl;
    ofstream<<"Index In Array: "<<iIndex<<std::endl;
    ofstream<<"Links number: "<<m_iRealSize<<std::endl;
    for (tvg_short_counter i = 0 ; i < std::min(m_iRealSize, GetMaxChainSize()) ; i++)
    {
        ofstream<<std::to_string(i+1)<<". Index "<<m_aChain[i].GetIndex()<<", Epoch "<<m_aChain[i].GetEpoch()<<std::endl;
    }



    return sizeof(SameNodeNVTemporalLink) * m_iRealSize;
}



