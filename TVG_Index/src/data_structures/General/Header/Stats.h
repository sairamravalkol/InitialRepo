//
// Created by norig on 9/6/15.
//

#ifndef TVG4TM_STATS_H
#define TVG4TM_STATS_H


#include "MyHistogram.h"
#include "tvg_index.h"

class Stats
{

private:
    //
    Stats()
    {
        m_pHistChain = std::make_unique<MyHistogram>(1000);
    }
    ~Stats()
    {

    }
    Stats(const Stats&) = delete;
    const Stats& operator=(const Stats&) = delete;


    std::unique_ptr<MyHistogram> m_pHistChain;


public:
    //
    static Stats* Instance()
    {
        static Stats* the_config = new Stats();
        return the_config;
    }




    void PrintHistogram()
    {
        m_pHistChain->Print("Chain_Histogram");
    }

    void AddToHistogram(tvg_index tVal)
    {
        m_pHistChain->AddElement(tVal);
    }


};

#endif //TVG4TM_STATS_H
