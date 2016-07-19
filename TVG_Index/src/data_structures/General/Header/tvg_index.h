//
// Created by norig on 9/6/15.
//

#ifndef TVG4TM_TVG_INDEX_H
#define TVG4TM_TVG_INDEX_H


#include <string>
#include <fstream>
#include "tvg_time.h"
#include "tvg_vertex_id.h"

#include "../../../../include/Globals.h"



class tvg_vertex_id;
class tvg_time;


class tvg_index
{
private:
    //
    unsigned long long m_lIndex;




    tvg_index(const tvg_vertex_id& lID) // do nothing, to prevent
    {

    }


    tvg_index(const tvg_time& lID) // do nothing, to prevent
    {

    }



public:
    //
    tvg_index()
    {
        m_lIndex = -1;
    }



    tvg_index(const unsigned long long & lIndex)
    {
        m_lIndex = lIndex;
    }

    tvg_index(const tvg_index& l)
    {
        m_lIndex = l.m_lIndex;
    }



    bool IsValid()
    {
        return (m_lIndex != -1);
    }


    unsigned long long Get() const
    {
        return m_lIndex;
    }




    operator unsigned long long() const
    {
        return  m_lIndex;
    }

    tvg_index& operator=(const tvg_index& other)
    {
        m_lIndex = other.m_lIndex;

        return *this;
    }

    unsigned long long operator() () const
    {
        return m_lIndex;
    }

    tvg_index& operator++()
    {
        m_lIndex++;
        return *this;
    }

    tvg_index operator++(int)
    {
        tvg_index tmp(*this);
        operator++();
        return tmp;
    }


    tvg_index& operator--()
    {
        m_lIndex--;
        return *this;
    }

    tvg_index operator--(int)
    {
        tvg_index tmp(*this);
        operator--();
        return tmp;
    }


    const tvg_index operator+(const tvg_index& l)
    {
        return tvg_index(*this) += l;
    }

    /*const tvg_index operator+(const tvg_index l)
    {
        return tvg_index(*this) += l;
    }*/


    /*const tvg_index operator-(const tvg_index l) const
    {
        return tvg_index(*this) -= l;
    }*/

    const tvg_index operator-(const tvg_index& l) const
    {
        return tvg_index(*this) -= l;
    }

    const tvg_index operator-(const tvg_short_counter& i) const
    {
        return tvg_index(*this) -= i;
    }




    const tvg_index operator*(const tvg_index l) const
    {
        return tvg_index(*this) *= l;
    }



    const tvg_index operator*(tvg_long_counter l) const
    {
        return tvg_index(*this) *= l;
    }



    const tvg_index operator/(const tvg_index l) const
    {
        return tvg_index(*this) /= l;
    }



    tvg_index& operator+=(const tvg_index& l)
    {
        m_lIndex += l.m_lIndex;
        return *this;
    }

    tvg_index& operator-=(const tvg_index& l)
    {
        m_lIndex -= l.m_lIndex;
        return *this;
    }

    tvg_index& operator-=(const tvg_short_counter& i)
    {
        m_lIndex -= i;
        return *this;
    }



    tvg_index& operator*=(const tvg_index& l)
    {
        m_lIndex *= l.m_lIndex;
        return *this;
    }

    tvg_index& operator/=(const tvg_index& l)
    {
        m_lIndex /= l.m_lIndex;
        return *this;
    }

    std::ostream& operator<<(std::ostream& os)
    {
        os<<GetString();
    }

    std::string GetString() const
    {
        return std::to_string(m_lIndex);
    }

    auto operator[](void*)
    {
        return m_lIndex;
    }

    bool operator()(const tvg_index& l1, const tvg_index& l2) const { return (l1.m_lIndex <= l2.m_lIndex); }

    bool operator< (const tvg_index& lhs) const{ return (m_lIndex < lhs.m_lIndex); }
    bool operator> (const tvg_index& lhs) const{ return (m_lIndex > lhs.m_lIndex); }
    bool operator> (const tvg_short_counter& lhs) const{ return (m_lIndex > lhs); }
    bool operator<=(const tvg_index& lhs) const{ return (m_lIndex <= lhs.m_lIndex); }
    bool operator>=(const tvg_index& lhs) const{ return (m_lIndex >= lhs.m_lIndex); }

    bool operator==(const tvg_index& lhs) const{ return (m_lIndex == lhs.m_lIndex); }
    bool operator!=(const tvg_index& lhs) const{ return !(*this  == lhs); }
};



#endif //TVG4TM_TVG_INDEX_H
