//
// Created by norig on 4/21/16.
//

#ifndef TVG4TM_PERFORMANCEMETER_H
#define TVG4TM_PERFORMANCEMETER_H


#include <iostream>

class PerformanceMeter
{
private:
    //
    long m_lBinarySearchTime;
    long m_lBinarySearchCounter;

    long m_lInterpolationSearchTime;
    long m_lInterpolationSearchCounter;

    long m_lSerialSearchTime;
    long m_lSerialSearchCounter;

    bool m_bPerformMeterData;

    PerformanceMeter()
    {
        Reset();
        m_bPerformMeterData = false;
    }

    ~PerformanceMeter(){}




public:
    //
    static PerformanceMeter* Instance()
    {
        static PerformanceMeter* global = new PerformanceMeter();
        return global;
    }

    void Reset()
    {
        m_lBinarySearchTime = 0;
        m_lBinarySearchCounter = 0;

        m_lInterpolationSearchTime = 0;
        m_lInterpolationSearchCounter = 0;

        m_lSerialSearchTime = 0;
        m_lSerialSearchCounter = 0;
    }


    bool DoPerformMeterData()
    {
        return m_bPerformMeterData;
    }


    void SetPerformMeterData(bool bPerformMeterData)
    {
        m_bPerformMeterData = bPerformMeterData;
    }

    void AddBinaryExecution(long lValue)
    {
        m_lBinarySearchTime += lValue;
        m_lBinarySearchCounter++;
    }
    void AddInterpolationExecution(long lValue)
    {
        m_lInterpolationSearchTime += lValue;
        m_lInterpolationSearchCounter++;
    }

    void AddSerialExecution(long lValue)
    {
        m_lSerialSearchTime += lValue;
        m_lSerialSearchCounter++;
    }


    void Print()
    {
        if (m_lBinarySearchCounter > 0)
            std::cout<<"Binary average = "<< double(m_lBinarySearchTime) / double(m_lBinarySearchCounter)<<std::endl;
        std::cout<<"Binary counter = "<< m_lBinarySearchCounter<<std::endl;

        if (m_lInterpolationSearchCounter > 0)
            std::cout<<"Interpolation average = "<< double(m_lInterpolationSearchTime) / double(m_lInterpolationSearchCounter)<<std::endl;
        std::cout<<"Interpolation counter = "<< m_lInterpolationSearchCounter<<std::endl;

        if (m_lSerialSearchCounter > 0)
            std::cout<<"Serial average = "<< double(m_lSerialSearchTime) / double(m_lSerialSearchCounter)<<std::endl;
        std::cout<<"Serial counter = "<< m_lSerialSearchCounter<<std::endl;
    }

    long GetBinarySearchTime()
    {
        return m_lBinarySearchTime;
    }

    long GetBinarySearchCounter()
    {
        return m_lBinarySearchCounter;
    }

    long GetInterpolationSearchTime()
    {
        return m_lInterpolationSearchTime;
    }

    long GetInterpolationSearchCounter()
    {
        return m_lInterpolationSearchCounter;
    }

    long GetSerialSearchTime()
    {
        return m_lSerialSearchTime;
    }

    long GetSerialSearchCounter()
    {
        return m_lSerialSearchCounter;
    }

};


#endif //TVG4TM_PERFORMANCEMETER_H
