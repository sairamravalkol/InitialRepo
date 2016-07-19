//
// Created by norig on 9/6/15.
//

#ifndef TVG4TM_TVG_VERTEX_ID_H
#define TVG4TM_TVG_VERTEX_ID_H


#include <string>
#include "tvg_time.h"
#include "tvg_index.h"

class tvg_index;
class tvg_time;

class tvg_vertex_id
{
private:
    //
    unsigned long long m_lVertexID;



    tvg_vertex_id(const tvg_index& lIndex)
    {

    }

    tvg_vertex_id(const tvg_time& lID) // do nothing, to prevent
    {

    }



public:
    //
    tvg_vertex_id()
    {
        m_lVertexID = -1;
    }

    tvg_vertex_id(unsigned long long lVertexID)
    {
        m_lVertexID = lVertexID;
    }

    tvg_vertex_id(const tvg_vertex_id& l)
    {
        m_lVertexID = l.m_lVertexID;
    }


    const tvg_vertex_id& operator=(const tvg_vertex_id& other)
    {
        m_lVertexID = other.m_lVertexID;

        return *this;
    }

    bool operator==(const tvg_vertex_id& lhs) const{ return (m_lVertexID == lhs.m_lVertexID); }
    bool operator!=(const tvg_vertex_id& lhs) const{ return !(*this  == lhs); }


    bool IsValid() const
    {
        return (m_lVertexID != -1);
    }


    const unsigned long long Get() const
    {
        return m_lVertexID;
    }

    std::string GetString() const
    {
        return std::to_string(m_lVertexID);
    }


    operator unsigned long long() const
    {
        return  m_lVertexID;
    }



    unsigned long long operator() () const
    {
        return m_lVertexID;
    }

    tvg_vertex_id& operator++()
    {
        m_lVertexID++;
        return *this;
    }

    tvg_vertex_id operator++(int)
    {
        tvg_vertex_id tmp(*this);
        operator++();
        return tmp;
    }


    tvg_vertex_id& operator--()
    {
        m_lVertexID--;
        return *this;
    }

    tvg_vertex_id operator--(int)
    {
        tvg_vertex_id tmp(*this);
        operator--();
        return tmp;
    }


    const tvg_vertex_id operator+(const tvg_vertex_id& l)
    {
        return tvg_vertex_id(*this) += l;
    }

    /*const tvg_vertex_id operator+(const tvg_vertex_id l)
    {
        return tvg_vertex_id(*this) += l;
    }*/


    /*const tvg_vertex_id operator-(const tvg_vertex_id l) const
    {
        return tvg_vertex_id(*this) -= l;
    }*/

    const tvg_vertex_id operator-(const tvg_vertex_id& l) const
    {
        return tvg_vertex_id(*this) -= l;
    }


    const tvg_vertex_id operator*(const tvg_vertex_id l) const
    {
        return tvg_vertex_id(*this) *= l;
    }


    const tvg_vertex_id operator/(const tvg_vertex_id l) const
    {
        return tvg_vertex_id(*this) /= l;
    }



    tvg_vertex_id& operator+=(const tvg_vertex_id& l)
    {
        m_lVertexID += l.m_lVertexID;
        return *this;
    }

    tvg_vertex_id& operator-=(const tvg_vertex_id& l)
    {
        m_lVertexID -= l.m_lVertexID;
        return *this;
    }

    tvg_vertex_id& operator*=(const tvg_vertex_id& l)
    {
        m_lVertexID *= l.m_lVertexID;
        return *this;
    }

    tvg_vertex_id& operator/=(const tvg_vertex_id& l)
    {
        m_lVertexID /= l.m_lVertexID;
        return *this;
    }

    std::ostream& operator<<(std::ostream& os)
    {
        os<<GetString();
    }


    auto operator[](void*)
    {
        return m_lVertexID;
    }

    bool operator()(const tvg_vertex_id& l1, const tvg_vertex_id& l2) const { return (l1.m_lVertexID <= l2.m_lVertexID); }

    bool operator< (const tvg_vertex_id& lhs) const{ return (m_lVertexID < lhs.m_lVertexID); }
    bool operator> (const tvg_vertex_id& lhs) const{ return (m_lVertexID > lhs.m_lVertexID); }
    bool operator<=(const tvg_vertex_id& lhs) const{ return (m_lVertexID <= lhs.m_lVertexID); }
    bool operator>=(const tvg_vertex_id& lhs) const{ return (m_lVertexID >= lhs.m_lVertexID); }


};



#endif //TVG4TM_TVG_VERTEX_ID_H
