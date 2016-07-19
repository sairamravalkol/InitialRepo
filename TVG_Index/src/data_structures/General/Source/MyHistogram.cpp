//
// Created by norig on 8/19/15.
//


#include <fstream>
#include <assert.h>
#include "../Header/MyHistogram.h"






MyHistogram::MyHistogram(unsigned int iSize, std::string strOutputFolder) : m_strOutputFolder(strOutputFolder), m_iMaxHistSize(iSize)
{

    m_iTotalAddedValues = 0;



    m_aHistogram = std::make_unique<unsigned int[]>(m_iMaxHistSize);
    for (int i = 0 ; i < m_iMaxHistSize ; i++)
        m_aHistogram[i] = 0;
}

MyHistogram::~MyHistogram()
{
}

void MyHistogram::AddElement(int iIndex)
{
    if ((iIndex < 0) || (iIndex >= m_iMaxHistSize))
    {
        assert(0);
        return;
    }

    m_aHistogram[iIndex]++;
    m_iTotalAddedValues++;

    if (iIndex > m_iMaxLocatedIndex)
        m_iMaxLocatedIndex = iIndex;
}


void MyHistogram::SeValue(int iIndex, int iValue)
{
    m_aHistogram[iIndex] = iValue;
    m_iTotalAddedValues += iValue;
}



void MyHistogram::Print(std::string strName, bool bSkipZeros)
{
    std::string strFileName(m_strOutputFolder + strName + ".txt");
    std::ofstream m_wFile(strFileName, std::ios::out);
    m_wFile<<"Histogram of size "<<m_iMaxHistSize<<'\n';
    m_wFile<<"Total number of words is : "<<m_iTotalAddedValues<<'\n';
    int iZeros = 0;
    for (unsigned int i = 0 ; i < m_iMaxLocatedIndex ; i++)
    {
        if (bSkipZeros)
        {
            if (m_aHistogram[i] == 0)
            {
                iZeros++;
                continue;
            }
            else
            {
                m_wFile<<'\n';
                m_wFile<<'\n';
                m_wFile<<'\n';


                for (int j = 0 ; j < iZeros ; j++)
                    m_wFile<<"---";

                m_wFile<<'\n';
                m_wFile<<'\n';
                m_wFile<<'\n';

                iZeros = 0;

                m_wFile<<"Index "<<i<< " = "<<m_aHistogram[i]<<'\n';
            }
        }

        else
        {
            m_wFile<<"Index "<<i<< " = "<<m_aHistogram[i]<<'\n';
            m_wFile<<'\n';
        }


    }

    m_wFile.close();
}