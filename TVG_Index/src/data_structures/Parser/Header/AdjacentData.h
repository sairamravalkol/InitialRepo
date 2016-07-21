//
// Created by norig on 6/24/15.
//

#ifndef TVG_IGOR_ADJACENTDATA_H
#define TVG_IGOR_ADJACENTDATA_H

#include <ctime>
#include <stdio.h>
#include <bitset>
#include "../../General/Header/tvg_vertex_id.h"
#include "../../General/Header/tvg_index.h"
#include "../../General/Header/tvg_time.h"
#include "../../../../include/Globals.h"
#include "Modification.h"


class AdjacentData
{
private:
    //
//    tvg_long_counter  m_lUID;

    tvg_vertex_id m_lID;
    tvg_time m_lConnectionEpoch;
//    bool m_bSource;
//    MODIFICATION_TYPE m_modificationType;

    std::bitset<8> m_aBoolData; // Bit 0 = IsSource; Bit 1: IsAddition

    tvg_index m_lIndexInData;


public:
    //
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////// Constractors
    //
    AdjacentData() : /*m_lUID(-1),*/ m_lID(-1), m_lConnectionEpoch(-1), m_lIndexInData(-1)
    {


    }

    AdjacentData(const tvg_long_counter&  lUID, tvg_vertex_id lID, tvg_time lConnectionEpoch, tvg_index lIndexInData, bool bSource) : /*m_lUID(lUID),*/ m_lID(lID), m_lConnectionEpoch(lConnectionEpoch), m_lIndexInData(lIndexInData)
    {
        m_aBoolData.set(0, bSource);
    }

    AdjacentData(const AdjacentData& elem) : /*m_lUID(elem.m_lUID),*/ m_lID(elem.m_lID), m_lConnectionEpoch(elem.m_lConnectionEpoch), m_lIndexInData(elem.m_lIndexInData)
    {
        m_aBoolData.set(0, elem.IsSource());
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////// Get
    //
    /*
    const tvg_long_counter&  GetUID() const
    {
        return m_lUID;
    }
*/


    tvg_vertex_id GetID()
    {
        return m_lID;
    }


    tvg_time GetEpoch()
    {
        return m_lConnectionEpoch;
    }


    bool IsSource() const
    {
        return m_aBoolData[0];
    }


    tvg_index GetIndexInData()
    {
        return m_lIndexInData;
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////  Set
    //
    /*
    void  SetUID(const tvg_long_counter& lUID)
    {
        m_lUID = lUID;
    }
*/


    void SetID(tvg_vertex_id lID)
    {
        m_lID = lID;
    }


    void SetEpoch(tvg_time& lConnectionEpoch)
    {
        m_lConnectionEpoch = lConnectionEpoch;
    }


    void SetIsSource(bool bSource)
    {
        m_aBoolData.set(0, bSource);
    }


    void SetIndexInData(tvg_index lIndexInData)
    {
        m_lIndexInData = lIndexInData;
    }


    void SetData(const tvg_long_counter&  lUID, tvg_time& lConnectionEpoch, tvg_vertex_id lID, tvg_index lIndexInData, bool bSource, MODIFICATION_TYPE modificationType)
    {
//        m_lUID = lUID;
        m_lConnectionEpoch = lConnectionEpoch;
        m_lID = lID;
        m_aBoolData.set(0, bSource);
        m_aBoolData.set(1, (modificationType == MODIFICATION_TYPE::ADDITION));

        m_lIndexInData = lIndexInData;
    }
};


#endif //TVG_IGOR_ADJACENTDATA_H
