//
// Created by norig on 6/24/15.
//

#ifndef TVG_IGOR_NVCHAINSTRUCTURE_H
#define TVG_IGOR_NVCHAINSTRUCTURE_H



#include <stdio.h>
#include <map>
#include <fstream>


#include "../../DRAM/Header/AdjacentDStructure.h"
#include "../../General/Header/Link.h"
//#include "../../../infrastructure/Configuration.h"

//#include "ChainAdditionalData.h"
#include "NVRepository.h"
#include "../../Graph/Header/MyVertexSelect.h"

///////////////////////////////////////////////////////////////////
//
/*
class NVChainElem
{
protected:
    //
    tvg_time m_tEpoch;
    NVVertexLink m_linkValue;


public:
    //
    NVChainElem(tvg_time tEpoch, NVVertexLink linkValue) : m_tEpoch(tEpoch), m_linkValue(linkValue)
    {


    }

    NVChainElem() : m_linkValue()
    {


    }

    NVChainElem(const NVChainElem& elem) : m_tEpoch(elem.m_tEpoch), m_linkValue(elem.m_linkValue)
    {


    }


    tvg_time GetEpoch()
    {
        return m_tEpoch;
    }

    NVVertexLink& GetLink()
    {
        return m_linkValue;
    }


    void SetEpoch(tvg_time tEpoch)
    {
        m_tEpoch = tEpoch;
    }

    void SetLink(NVVertexLink* pLink)
    {
        m_linkValue = *pLink;
    }


};

*/

class NVRepository;

///////////////////////////////////////////////////////////////////
//
class NVChainStructure
{
protected:
    //

    // IN: solve static problem
    //
    //   static int m_iChainSize = Configuration::Instance()->GetIntegerItem("data-structure.chain-structure-size");
    //
    constexpr static tvg_short_counter m_iMaxChainSize = 10;


//    std::unique_ptr<NVLink> m_aAdditionalLinks;

 //   std::unique_ptr<NVTemporalLink[]> m_aAdditionalLinks;
 //   unsigned int m_iAdditionalLinksSize;


    tvg_short_counter m_iRealSize;
    SameNodeNVTemporalLink m_aChain[m_iMaxChainSize];




public:
    //
    NVChainStructure();
    NVChainStructure(Vertica::ServerInterface& log);
    ~NVChainStructure();

    const SameNodeNVTemporalLink& GetChainItem(int i)
    {
        return m_aChain[i];
    }

    static const tvg_short_counter GetMaxChainSize()
    {
        return m_iMaxChainSize;
    }

    NVChainStructure(const NVChainStructure& elem);
    const NVChainStructure & operator=(const NVChainStructure & elem);


    tvg_index GetSuccessor(const tvg_index& lCurr, const tvg_time& tEndEpoch, NVRepository& pRepository);
    tvg_index GetSuccessor_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch, NVRepository& pRepository);
    tvg_index GetSuccessor_NVRAM(const tvg_index& lCurr, const tvg_time& tEndEpoch, MyVertexSelect& pVertex);

    tvg_short_counter GetRealSize()
    {
        return m_iRealSize;
    }


    void SetData(std::unique_ptr<AdjacentDStructure> pCompleteRes, const tvg_vertex_id& lID, NVRepository& pRepository);



    tvg_long_counter Submit(std::ofstream& ofstream, unsigned long iIndex);

//    static std::pair<int,int> AddToPair(const std::pair<int, int> &p, int amount)
//    {
//        return std::make_pair(p.first,p.second+amount);
//    }
//    static int GetFromPair(const std::pair<int, int> &p)
//    {
//        return p.second;
//    }
};


#endif //TVG_IGOR_NVCHAINSTRUCTURE_H
