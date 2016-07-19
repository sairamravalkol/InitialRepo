//
// Created by norig on 9/6/15.
//

#ifndef TVG4TM_NEIGHBORMETADATA_H
#define TVG4TM_NEIGHBORMETADATA_H

#include "../../../../include/Globals.h"
#include "Link.h"


class SameNodeNVLink;


class NeighborMetaData
{
protected:
    //
    tvg_vertex_id m_lID;
    tvg_counter m_lCounter;
    tvg_time m_tEpoch;

    SameNodeNVLink m_location;


public:
    //

    NeighborMetaData(const tvg_vertex_id& lID, const SameNodeNVLink& location, const tvg_time& tEpoch) : m_lID(lID), m_location(location), m_tEpoch(tEpoch), m_lCounter(1)
    {

    }

    NeighborMetaData(const tvg_vertex_id& lID, const tvg_time& tEpoch, const tvg_counter& lCounter) : m_lID(lID), m_tEpoch(tEpoch), m_lCounter(lCounter)
    {

    }

    NeighborMetaData(const tvg_vertex_id& lID, const SameNodeNVLink& location, const tvg_time& tEpoch, const tvg_counter& lCounter) : m_lID(lID), m_location(location), m_tEpoch(tEpoch), m_lCounter(lCounter)
    {

    }

    void SetData(const tvg_vertex_id& lID, const SameNodeNVLink& location, const tvg_time& tEpoch, const tvg_counter& lCounter)
    {
        m_lID = lID;
        m_location = location;
        m_tEpoch = tEpoch;
        m_lCounter = lCounter;
    }


    const tvg_vertex_id& GetID()
    {
        return m_lID;
    }

    const tvg_counter& GetCounter()
    {
        return m_lCounter;
    };

    const tvg_time& GetEpoch()
    {
        return m_tEpoch;
    }


    const SameNodeNVLink& GetLocation()
    {
        return m_location;
    }


    void IncreaseCount()
    {
        m_lCounter++;
    }

    void DecreaseCount()
    {
        m_lCounter--;
    }

};


#endif //TVG4TM_NEIGHBORMETADATA_H
