//
// Created by norig on 6/21/15.
//

#ifndef TVG_IGOR_LINK_H
#define TVG_IGOR_LINK_H

#include <ctime>
#include <stdio.h>
#include <fstream>
#include <bits/unique_ptr.h>
#include "../../../../include/Globals.h"
#include "tvg_index.h"
#include "tvg_vertex_id.h"
#include "tvg_time.h"



//
///////////////////////////////////////////////////////////////////
//
/*
class Pointer : public Link
{
private:
	//
	long m_lIndex;


	void* m_pPointer;



public:
	//
	SimpleLink(void* pPointer)
	{
		m_pPointer = pPointer;
	}

    void* GetLinkedVertex(
	{
		return m_pPointer;
	}


};
*/

//
///////////////////////////////////////////////////////////////////
//
/*
class SimpleVertexLink : public Link
{
protected:
    //
    long m_lVertexID;



public:
    //
	SimpleVertexLink()
	{


	}


	SimpleVertexLink(long lVertexID, long lIndex)
    {
        m_lVertexID = lVertexID;
        m_lIndex = lIndex;
    }

	long GetVertexID() const
	{
		return m_lVertexID;
	}

	void SetVertexID(long lVertexID)
	{
		m_lVertexID = lVertexID;
	}


};
*/
//
///////////////////////////////////////////////////////////////////
//


//class tvg_vertex_id;


class SameNodeNVLink //: public Link
{
protected:
	//
	tvg_index m_lIndex;



public:
	//
	SameNodeNVLink()
	{

	}
	virtual ~SameNodeNVLink()
	{

	}

	SameNodeNVLink(const tvg_index& lIndex) : m_lIndex(lIndex)
	{


	}

	SameNodeNVLink(const std::unique_ptr<SameNodeNVLink>& sLink)
	{
		m_lIndex = sLink->GetIndex();
	}


	SameNodeNVLink(const SameNodeNVLink& sLink)
	{
		m_lIndex = sLink.GetIndex();
	}

	const SameNodeNVLink& operator=(const SameNodeNVLink& sLink)
	{
		m_lIndex = sLink.m_lIndex;
		return *this;
	}

	bool operator< (const SameNodeNVLink& other) const
	{
		return (m_lIndex < other.m_lIndex);
	}


	bool operator==(const SameNodeNVLink& other) const
	{
		return (m_lIndex == other.m_lIndex);
	}

	bool operator!=(const SameNodeNVLink& other) const
	{
		return !(*this  == other);
	}




	const tvg_index GetIndex() const
	{
		return m_lIndex;
	}



	void SetIndex(tvg_index lIndex)
	{
		m_lIndex = lIndex;
	}


	virtual void SetData(const tvg_index& lIndex)
	{
		m_lIndex = lIndex;
	}


	virtual void SetData(const SameNodeNVLink& sLink)
	{
		m_lIndex = sLink.GetIndex();
	}

	virtual void SetEmpty()
	{
		m_lIndex = -1;
	}

	virtual tvg_long_counter Submit(std::ofstream& file)
	{
		file<<"---------------------------"<<std::endl;
		file<<"Index: "<<m_lIndex.GetString()<<std::endl;

		return sizeof(*this);
	}



};





class SameNodeNVTemporalLink : public SameNodeNVLink
{
protected:
	//
	tvg_time m_tEpoch;



public:
	//

	virtual tvg_long_counter Submit(std::ofstream& file)
	{
		SameNodeNVLink::Submit(file);
		file<<"Epoch: "<<m_tEpoch.GetString()<<std::endl;

		return sizeof(*this);
	}



	SameNodeNVTemporalLink()
	{


	}
	virtual ~SameNodeNVTemporalLink()
	{


	}

	SameNodeNVTemporalLink(tvg_index lIndex, tvg_time tEpoch) : SameNodeNVLink(lIndex), m_tEpoch(tEpoch)
	{


	}


	tvg_time GetEpoch()
	{
		return m_tEpoch;
	}


	void SetEpoch(tvg_time tEpoch)
	{
		m_tEpoch = tEpoch;
	}


	void SetLink(const SameNodeNVLink& link)
	{
		SetIndex(link.GetIndex());
	}

	void SetLink(const std::unique_ptr<SameNodeNVTemporalLink>& pLink)
	{
		SetIndex(pLink->GetIndex());
		SetEpoch(pLink->GetEpoch());
	}



	const SameNodeNVTemporalLink& operator=(const SameNodeNVTemporalLink& sLink)
	{
		SameNodeNVLink::operator=(sLink);
		m_tEpoch = sLink.m_tEpoch;
		return *this;
	}

	bool operator< (const SameNodeNVTemporalLink& other) const
	{
		if (SameNodeNVLink::operator==(other))
		{
			return (m_tEpoch < other.m_tEpoch);
		}
		else
			return (SameNodeNVLink::operator<(other));
	}


	bool operator==(const SameNodeNVTemporalLink& other) const
	{
		return ((SameNodeNVLink::operator==(other)) && (m_tEpoch == other.m_tEpoch));
	}

	bool operator!=(const SameNodeNVTemporalLink& other) const
	{
		return !(*this  == other);
	}



/*
	static std::pair<int,int> AddToPair(const std::pair<int, int> &p, int amount) {
		return std::make_pair(p.first+amount,p.second);
	}
	static int GetFromPair(const std::pair<int, int> &p){
		return p.first;
	}

 */
};





















/*

class OtherNodeNVLink //: public Link
{
protected:
	//
	tvg_vertex_id m_lVertexID;
	tvg_index m_lIndex;



public:
	//
	OtherNodeNVLink()
	{

	}
	virtual ~OtherNodeNVLink()
	{

	}

	OtherNodeNVLink(const tvg_vertex_id& lVertexID, const tvg_index& lIndex) : m_lVertexID(lVertexID), m_lIndex(lIndex)
	{


	}

	OtherNodeNVLink(const std::unique_ptr<OtherNodeNVLink>& sLink)
	{
		m_lVertexID = sLink->GetVertexID();
		m_lIndex = sLink->GetIndex();
	}


	OtherNodeNVLink(const OtherNodeNVLink& sLink)
	{
		m_lVertexID = sLink.GetVertexID();
		m_lIndex = sLink.GetIndex();
	}

	const OtherNodeNVLink& operator=(const OtherNodeNVLink& sLink)
	{
		m_lVertexID = sLink.m_lVertexID;
		m_lIndex = sLink.m_lIndex;
		return *this;
	}

	bool operator< (const OtherNodeNVLink& other) const
	{
		if (m_lVertexID == other.m_lVertexID)
		{
			return (m_lIndex < other.m_lIndex);
		}
		else
			return (m_lVertexID < other.m_lVertexID);
	}


	bool operator==(const OtherNodeNVLink& other) const
	{
		return ((m_lVertexID == other.m_lVertexID) && (m_lIndex == other.m_lIndex));
	}

	bool operator!=(const OtherNodeNVLink& other) const{ return !(*this  == other); }





	const tvg_index GetIndex() const
	{
		return m_lIndex;
	}
	const tvg_vertex_id& GetVertexID() const
	{
		return m_lVertexID;
	}





	void SetIndex(tvg_index lIndex)
	{
		m_lIndex = lIndex;
	}
	void SetVertexID(tvg_vertex_id lVertexID)
	{
		m_lVertexID = lVertexID;
	}

	virtual void SetData(const tvg_vertex_id& lVertexID, const tvg_index& lIndex = -1)
	{
		m_lVertexID = lVertexID;
		m_lIndex = lIndex;
	}

	virtual void SetData(const OtherNodeNVLink& sLink)
	{
		m_lVertexID = sLink.GetVertexID();
		m_lIndex = sLink.GetIndex();
	}

	virtual void SetEmpty()
	{
		m_lVertexID = -1;
		m_lIndex = -1;
	}

	virtual tvg_long_counter Submit(std::ofstream& file)
	{
		file<<"---------------------------"<<std::endl;
		file<<"VertexID: "<<m_lVertexID.GetString()<<std::endl;
		file<<"Index: "<<m_lIndex.GetString()<<std::endl;

		return sizeof(*this);
	}



};



class NVTemporalLink : public OtherNodeNVLink
{
protected:
	//
	tvg_time m_tEpoch;




public:
	//

	virtual tvg_long_counter Submit(std::ofstream& file)
	{
		OtherNodeNVLink::Submit(file);
		file<<"Epoch: "<<m_tEpoch.GetString()<<std::endl;

		return sizeof(*this);
	}



	NVTemporalLink()
	{


	}
	virtual ~NVTemporalLink()
	{


	}

	NVTemporalLink(tvg_vertex_id lVertexID, tvg_index lIndex, tvg_time tEpoch) : OtherNodeNVLink(lVertexID, lIndex), m_tEpoch(tEpoch)
	{


	}


	tvg_time GetEpoch()
	{
		return m_tEpoch;
	}


	void SetEpoch(tvg_time tEpoch)
	{
		m_tEpoch = tEpoch;
	}


	void SetLink(const OtherNodeNVLink& link)
	{
		SetIndex(link.GetIndex());
		SetVertexID(link.GetVertexID());
	}

	void SetLink(const std::unique_ptr<NVTemporalLink>& pLink)
	{
		SetIndex(pLink->GetIndex());
		SetVertexID(pLink->GetVertexID());
		SetEpoch(pLink->GetEpoch());
	}



	const NVTemporalLink& operator=(const NVTemporalLink& sLink)
	{
		OtherNodeNVLink::operator=(sLink);
		m_tEpoch = sLink.m_tEpoch;
		return *this;
	}

	bool operator< (const NVTemporalLink& other) const
	{
		if (OtherNodeNVLink::operator==(other))
		{
			return (m_tEpoch < other.m_tEpoch);
		}
		else
			return (OtherNodeNVLink::operator<(other));
	}


	bool operator==(const NVTemporalLink& other) const
	{
		return ((OtherNodeNVLink::operator==(other)) && (m_tEpoch == other.m_tEpoch));
	}

	bool operator!=(const NVTemporalLink& other) const{ return !(*this  == other); }


};
*/



class NVIntervalLink : public SameNodeNVLink
{
protected:
	//
	tvg_index m_lSize;


public:
	//
	NVIntervalLink()
	{


	}

	virtual ~NVIntervalLink()
	{


	}

	NVIntervalLink(tvg_index lIndex, tvg_index lSize) : SameNodeNVLink(lIndex), m_lSize(lSize)
	{


	}


	void SetData(tvg_index lIndex, tvg_index lSize)
	{
		SameNodeNVLink::SetData(lIndex);
		m_lSize = lSize;

	}



	void SetSize(tvg_index lSize)
	{
		m_lSize = lSize;
	}


	void SetLink(const SameNodeNVLink& link)
	{
		SetIndex(link.GetIndex());
	}

	void SetLink(std::unique_ptr<SameNodeNVLink> pLink)
	{
		SetIndex(pLink->GetIndex());
	}


	const NVIntervalLink& operator=(const NVIntervalLink& sLink)
	{
		SameNodeNVLink::operator=(sLink);
		m_lSize = sLink.m_lSize;
		return *this;
	}


	bool operator< (const NVIntervalLink& other) const
	{
		if (SameNodeNVLink::operator==(other))
		{
			return (m_lSize < other.m_lSize);
		}
		else
			return (SameNodeNVLink::operator<(other));
	}


	bool operator==(const NVIntervalLink& other) const
	{
		return ((SameNodeNVLink::operator==(other)) && (m_lSize == other.m_lSize));
	}

	bool operator!=(const NVIntervalLink& other) const{ return !(*this  == other); }


	tvg_index GetSize() const
	{
		return m_lSize;
	}


	virtual tvg_long_counter Submit(std::ofstream& file)
	{
		SameNodeNVLink::Submit(file);
		file<<"Size: "<<m_lSize.GetString()<<std::endl;

		return sizeof(*this);
	}


};








class NVLinkPairInterval //: public Link
{
protected:
	//
	SameNodeNVLink m_linkFirst;
	NVIntervalLink m_linkSecond;


public:
	//
	NVLinkPairInterval()
	{

	}

	NVLinkPairInterval(const SameNodeNVLink &linkFirst, const NVIntervalLink &linkSecond) : m_linkFirst(linkFirst), m_linkSecond(linkSecond)
	{

	}

	virtual ~NVLinkPairInterval()
	{

	}



	const SameNodeNVLink& GetFirstLink()
	{
		return m_linkFirst;
	}

	const NVIntervalLink& GetSecondLink()
	{
		return m_linkSecond;
	}



	void SetData(const SameNodeNVLink &linkFirst, const NVIntervalLink &linkSecond)
	{
		m_linkFirst.SetData(linkFirst.GetIndex());
		m_linkSecond.SetData(linkSecond.GetIndex(), linkSecond.GetSize());
	}


	const NVLinkPairInterval& operator=(const NVLinkPairInterval& sLink)
	{
		m_linkFirst=sLink.m_linkFirst;
		m_linkSecond=sLink.m_linkSecond;
		return *this;
	}

	bool operator< (const NVLinkPairInterval& other) const
	{
		if (m_linkFirst==other.m_linkFirst)
		{
			return (m_linkSecond < other.m_linkSecond);
		}
		else
			return (m_linkFirst < other.m_linkFirst);
	}


	bool operator==(const NVLinkPairInterval& other) const
	{
		return ((m_linkFirst==other.m_linkFirst) && (m_linkSecond == other.m_linkSecond));
	}

	bool operator!=(const NVLinkPairInterval& other) const{ return !(*this  == other); }



	virtual tvg_long_counter Submit(std::ofstream& file)
	{
		file<<"---------------------------"<<std::endl;
		file<<"Index="<<m_linkFirst.GetIndex().GetString()<<std::endl;

		file<<"Add Chain: Index="<<m_linkSecond.GetIndex().GetString()<<", Size: "<<m_linkSecond.GetSize().GetString()<<std::endl;

		return sizeof(*this);
	}


	/*
	static std::pair<int,int> AddToPair(const std::pair<int, int> &p, int amount) {
		return std::make_pair(p.first+amount,p.second);
	}
	static int GetFromPair(const std::pair<int, int> &p){
		return p.first;
	}
*/
};


/*

class NVChainLink //: public Link
{
protected:
	//
	long m_lVertexChainID;

	long m_lIndex;




public:
	//
	NVChainLink()
	{


	}

	NVChainLink(long lVertexChainID, long lIndex)
	{
		m_lVertexChainID = lVertexChainID;
		m_lIndex = lIndex;
	}



	long GetIndex() const
	{
		return m_lIndex;
	}

	long GetVertexID()
	{
		return m_lVertexChainID;
	}



	void SetIndex(long lIndex)
	{
		m_lIndex = lIndex;
	}
	void SetVertexChainID(long lVertexID)
	{
		m_lVertexChainID = lVertexID;
	}



	void SetLink()
	{

	}
};



class NVChainTemporalLink : public NVVertexLink
{
protected:
	//
	tvg_time m_tEpoch;




public:
	//
	NVVertexTemporalLink()
	{


	}

	NVVertexTemporalLink(long lVertexID, long lIndex, tvg_time tEpoch) : NVVertexLink(lVertexID, lIndex), m_tEpoch(tEpoch)
	{


	}


	long GetEpoch()
	{
		return m_tEpoch;
	}


	void SetEpoch(tvg_time tEpoch)
	{
		m_tEpoch = tEpoch;
	}


};

*/


/*
class SameListLink : public Link
{
public:
	//
	SameListLink(std::map<long, MyVertex*>* mapIDNodes)
	{


	}

public:
	//

	AdjacentDVertex* GetLinkedVertex()
	{

	}

};



//
///////////////////////////////////////////////////////////////////
//


class OtherListLink : public Link
{
private:
	//
	MyVertex* m_pVertex;


public:
	//
	OtherListLink(MyVertex* pVertex)
	{
		m_pVertex = pVertex;
	}


};


*/



#endif //TVG_IGOR_LINK_H
