//
// Created by norig on 8/9/15.
//

#ifndef TVG4TM_EDGE_H
#define TVG4TM_EDGE_H

#include<iostream>
#include "../../General/Header/tvg_time.h"
#include "../../../../include/Globals.h"




class MyEdge
{
protected:
    //<iostream
    const tvg_vertex_id m_lSource;
    const tvg_vertex_id m_lDestination;

    static const bool m_bDirected = false;


public:
    //
    MyEdge(tvg_vertex_id lSource, tvg_vertex_id lDestination) : m_lSource(lSource), m_lDestination(lDestination)
    {

    }

    MyEdge(const MyEdge& e) : m_lSource(e.m_lSource), m_lDestination(e.m_lDestination)
    {

    }

    virtual ~MyEdge()
    {

    }




    tvg_vertex_id GetSource() const
    {
        return m_lSource;
    }

    tvg_vertex_id GetDestination() const
    {
        return m_lDestination;
    }



    bool operator< (const MyEdge& lhs) const
    {
        if (m_lSource < lhs.m_lSource)
        {
            return true;
        }
        else
        {
            if ((m_lSource == lhs.m_lSource) && (m_lDestination < lhs.m_lDestination))
                return true;
            else
                return false;
        }
    }


    bool operator==(const MyEdge& elem) const
    {
        if ((m_lSource == elem.m_lSource) && (m_lDestination == elem.m_lDestination))
            return true;

        if ((!m_bDirected) && (m_lSource == elem.m_lDestination) && (m_lDestination == elem.m_lSource))
            return true;


        return false;
    }
    bool operator!=(const MyEdge& lhs) const{ return !(*this  == lhs); }

};



class MyEdgeTemporal : public MyEdge
{
protected:
    //
    const tvg_time m_tEpoch;



public:
    //
    MyEdgeTemporal(tvg_vertex_id lSource, tvg_vertex_id lDestination, tvg_time tEpoch) : MyEdge(lSource, lDestination), m_tEpoch(tEpoch)
    {

    }


    MyEdgeTemporal(const MyEdgeTemporal& e) : m_tEpoch(e.m_tEpoch), MyEdge(e)
    {

    }


    virtual ~MyEdgeTemporal()
    {

    }


    tvg_time GetEpoch() const
    {
        return m_tEpoch;
    }


    bool operator< (const MyEdgeTemporal& lhs) const
    {
        if (MyEdge::operator<(lhs))
            return true;

        if (MyEdge::operator==(lhs))
        {
            if (m_tEpoch < lhs.m_tEpoch)
                return true;
            else
                return false;
        }

        return false;
    }


    bool operator==(const MyEdgeTemporal& lhs) const
    {
        if ((MyEdge::operator==(lhs)) && (m_tEpoch == lhs.m_tEpoch))
            return true;
        else
            return false;
    }
    bool operator!=(const MyEdgeTemporal& lhs) const{ return !(*this  == lhs); }


};


class MyEdgeTemporalAction : public MyEdgeTemporal
{
protected:
    //
    bool m_bAddition;



public:
    //
    MyEdgeTemporalAction(tvg_vertex_id lSource, tvg_vertex_id lDestination, tvg_time tEpoch, bool bAddition) : MyEdgeTemporal(lSource, lDestination, tEpoch), m_bAddition(bAddition)
    {

    }


    MyEdgeTemporalAction(const MyEdgeTemporalAction& e) : m_bAddition(e.m_bAddition), MyEdgeTemporal(e)
    {

    }


    virtual ~MyEdgeTemporalAction()
    {

    }


    bool IsAddition() const
    {
        return m_bAddition;
    }


    bool operator< (const MyEdgeTemporalAction& lhs) const
    {
        return (MyEdgeTemporal::operator<(lhs));
    }


    bool operator==(const MyEdgeTemporalAction& lhs) const
    {
        if ((MyEdgeTemporal::operator==(lhs)) && (m_bAddition == lhs.m_bAddition))
            return true;
        else
            return false;
    }
    bool operator!=(const MyEdgeTemporalAction& lhs) const{ return !(*this  == lhs); }


    void PrintToScreen()
    {
        std::cout<<"source="<<m_lSource.GetString()<<", destination="<<m_lDestination.GetString()<<", epoch="<<m_tEpoch.GetString()<<std::endl;
    }

};



class MyEdgeAdvanced : public MyEdgeTemporal
{
protected:
    //
    long m_lFrequency;
    int m_iDepth;
    const bool m_bIsLastEdge;

public:
    //
    MyEdgeAdvanced(tvg_vertex_id lSource, tvg_vertex_id lDestination, tvg_time tEpoch, int iDepth/* = 0*/, bool bIsLastEdge/* = false*/, long lFrequency/* = 1*/) : MyEdgeTemporal(lSource, lDestination, tEpoch), m_iDepth(iDepth), m_lFrequency(lFrequency), m_bIsLastEdge(bIsLastEdge)
    {


    }


    MyEdgeAdvanced(const MyEdgeAdvanced& e) : MyEdgeTemporal(e), m_bIsLastEdge(e.m_bIsLastEdge), m_iDepth(e.m_iDepth), m_lFrequency(e.m_lFrequency)
    {

    }


    virtual ~MyEdgeAdvanced()
    {

    }

    int GetDepth() const
    {
        return m_iDepth;
    }

    long GetFrequency() const
    {
        return m_lFrequency;
    }


    bool IsLastEdge() const
    {
        return m_bIsLastEdge;
    }


    void SetDepth(int iDepth)
    {
        m_iDepth = iDepth;
    }

    void SetFrequency(long lFrequency)
    {
        m_lFrequency = lFrequency;
    }

    void IncreaseFrequency()
    {
        m_lFrequency++;
    }

    void DecreaseFrequency()
    {
        m_lFrequency--;
    }


    bool operator< (const MyEdgeAdvanced& lhs) const
    {
        if (MyEdgeTemporal::operator<(lhs))
            return true;

        if (MyEdgeTemporal::operator==(lhs))
        {
            if (m_iDepth == lhs.m_lFrequency)
            {
                if (m_lFrequency < lhs.m_lFrequency)
                    return true;
                else
                    return false;
            }
            else
                return (m_iDepth < lhs.m_lFrequency);
        }

        return false;
    }


    bool operator==(const MyEdgeAdvanced& lhs) const
    {
        if ((MyEdgeTemporal::operator==(lhs))) // && (m_lFrequency == lhs.m_lFrequency)) - meanwhile without the frequency
            return true;
        else
            return false;
    }

    bool operator!=(const MyEdgeAdvanced& lhs) const{ return !(*this  == lhs); }

};


class MyEdgeCompare
{
public:
    //
    bool operator()(const MyEdgeAdvanced& a, const MyEdgeAdvanced& b) const
    {

        if ((a.GetSource() == a.GetDestination()) || (b.GetSource() == b.GetDestination()))
        {
            if (std::min(a.GetSource(), a.GetDestination()) == std::min(b.GetSource(), b.GetDestination()))
                return (std::max(a.GetSource(), a.GetDestination()) < std::max(b.GetSource(), b.GetDestination()));
            else
                return (std::min(a.GetSource(), a.GetDestination()) < std::min(b.GetSource(), b.GetDestination()));
        }


        if (a.GetSource() < a.GetDestination())
        {
            if (b.GetSource() < b.GetDestination())
            {
                if (((a.GetSource() <= b.GetSource()) && (a.GetDestination() < b.GetDestination())) ||
                    ((a.GetSource() < b.GetSource()) && (a.GetDestination() <= b.GetDestination())))
                    return true;
                else
                    return false;
            }
            else
            {
                if (((a.GetSource() <= b.GetDestination()) && (a.GetDestination() < b.GetSource())) ||
                    ((a.GetSource() < b.GetDestination()) && (a.GetDestination() <= b.GetSource())))
                    return true;
                else
                    return false;
            }
        }
        else
        {
            if (b.GetSource() < b.GetDestination())
            {
                if (((a.GetSource() <= b.GetDestination()) && (a.GetDestination() < b.GetSource())) ||
                    ((a.GetSource() < b.GetDestination()) && (a.GetDestination() <= b.GetSource())))
                    return true;
                else
                    return false;
            }
            else
            {
                if (((a.GetSource() <= b.GetSource()) && (a.GetDestination() < b.GetDestination())) ||
                    ((a.GetSource() < b.GetSource()) && (a.GetDestination() <= b.GetDestination())))
                    return true;
                else
                    return false;
            }
        }

//        return (a != b);




    }
};





#endif //TVG4TM_EDGE_H
