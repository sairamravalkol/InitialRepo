//
// Created by norig on 9/6/15.
//

#ifndef TVG4TM_TVG_TIME_H
#define TVG4TM_TVG_TIME_H

#include <string>
#include "tvg_index.h"


class tvg_index;
class tvg_vertex_id;


class tvg_time
{
    //
    unsigned long long m_lEpoch;


    tvg_time(const tvg_vertex_id& lID) = delete;// do nothing, to prevent
    //{

    //}


    tvg_time(const tvg_index& lID) = delete; // do nothing, to prevent
    //{

    //}



public:
    //
    tvg_time()
    {
        m_lEpoch = -1;
    }



    tvg_time(unsigned long long lIndex)
    {
        m_lEpoch = lIndex;
    }

    tvg_time(const tvg_time& l)
    {
        m_lEpoch = l.m_lEpoch;
    }


    bool IsValid()
    {
        return (m_lEpoch != -1);
    }


    unsigned long long Get() const
    {
        return m_lEpoch;
    }

    std::string GetString() const
    {
        return std::to_string(m_lEpoch);
    }


    operator unsigned long long() const
    {
        return  m_lEpoch;
    }

    tvg_time& operator=(const tvg_time& other)
    {
        m_lEpoch = other.m_lEpoch;

        return *this;
    }

    unsigned long long operator() () const
    {
        return m_lEpoch;
    }

    tvg_time& operator++()
    {
        m_lEpoch++;
        return *this;
    }

    tvg_time operator++(int)
    {
        tvg_time tmp(*this);
        operator++();
        return tmp;
    }


    tvg_time& operator--()
    {
        m_lEpoch--;
        return *this;
    }

    tvg_time operator--(int)
    {
        tvg_time tmp(*this);
        operator--();
        return tmp;
    }


    const tvg_time operator+(const tvg_time& l)
    {
        return tvg_time(*this) += l;
    }


    const tvg_time operator+(int i)
    {
        return tvg_time(*this) += i;
    }

    /*const tvg_time operator+(const tvg_time l)
    {
        return tvg_time(*this) += l;
    }*/


    /*const tvg_time operator-(const tvg_time l) const
    {
        return tvg_time(*this) -= l;
    }*/

    const tvg_time operator-(const tvg_time& l) const
    {
        return tvg_time(*this) -= l;
    }


    const tvg_time operator*(const tvg_time l) const
    {
        return tvg_time(*this) *= l;
    }


    const tvg_time operator/(const tvg_time l) const
    {
        return tvg_time(*this) /= l;
    }



    tvg_time& operator+=(const tvg_time& l)
    {
        m_lEpoch += l.m_lEpoch;
        return *this;
    }

    tvg_time& operator-=(const tvg_time& l)
    {
        m_lEpoch -= l.m_lEpoch;
        return *this;
    }

    tvg_time& operator*=(const tvg_time& l)
    {
        m_lEpoch *= l.m_lEpoch;
        return *this;
    }

    tvg_time& operator/=(const tvg_time& l)
    {
        m_lEpoch /= l.m_lEpoch;
        return *this;
    }

    std::ostream& operator<<(std::ostream& os)
    {
        os<<GetString();
    }


    auto operator[](void*)
    {
        return m_lEpoch;
    }

    bool operator()(const tvg_time& l1, const tvg_time& l2) const { return (l1.m_lEpoch <= l2.m_lEpoch); }

    bool operator< (const tvg_time& lhs) const{ return (m_lEpoch < lhs.m_lEpoch); }
    bool operator> (const tvg_time& lhs) const{ return (m_lEpoch > lhs.m_lEpoch); }
    bool operator<=(const tvg_time& lhs) const{ return (m_lEpoch <= lhs.m_lEpoch); }
    bool operator>=(const tvg_time& lhs) const{ return (m_lEpoch >= lhs.m_lEpoch); }

    bool operator==(const tvg_time& lhs) const{ return (m_lEpoch == lhs.m_lEpoch); }
    bool operator!=(const tvg_time& lhs) const{ return !(*this  == lhs); }


};


#endif //TVG4TM_TVG_TIME_H
