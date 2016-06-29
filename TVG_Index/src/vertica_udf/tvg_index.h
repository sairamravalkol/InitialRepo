//
// Created by hayuney on 27/06/16.
//

#ifndef TVG_INDEX_TVG_INDEX_H
#define TVG_INDEX_TVG_INDEX_H

//#include <cstdio>
#include <string>

class tvg_index {
private:

    unsigned long long m_lIndex;

public:

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
    const unsigned long long get_index()
    {
        return m_lIndex;
    }

    const unsigned long long Get() const
    {
        return m_lIndex;
    }

    std::string GetString() const
    {
        return std::to_string(m_lIndex);
    }


    operator unsigned long long() const
    {
        return  m_lIndex;
    }

};


#endif //TVG_INDEX_TVG_INDEX_H
