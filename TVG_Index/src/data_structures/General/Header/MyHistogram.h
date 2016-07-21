//
// Created by norig on 8/19/15.
//

#ifndef TVG4TM_MYHISTOGRAM_H
#define TVG4TM_MYHISTOGRAM_H


#include <stdio.h>
#include <string>
#include <bits/unique_ptr.h>

class MyHistogram
{
public:
	//
	MyHistogram(unsigned int iSize, std::string strOutputFoled = "/home/norig/tvg/Tvg4Tm/log/Output/");
	~MyHistogram();
	void AddElement(int iIndex);
	void SeValue(int iIndex, int iValue);

	void Print(std::string strName, bool bSkipZeros = true);



private:
	//
	std::unique_ptr<unsigned int[]> m_aHistogram;
	const unsigned int m_iMaxHistSize;

	const std::string m_strOutputFolder;

	int m_iMaxLocatedIndex;
	int m_iTotalAddedValues;


};


#endif //TVG4TM_MYHISTOGRAM_H
