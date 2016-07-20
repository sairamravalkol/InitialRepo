//
// Created by norig on 6/21/15.
//

#ifndef TVG_IGOR_MYVERTEX_H
#define TVG_IGOR_MYVERTEX_H


//#include <log4cpp/Category.hh>
#include <Vertica.h>
#include "../../DRAM/Header/AdjacencyDList.h"
#include "../../NVRAM/Header/NVRepository.h"
//#include "../../../infrastructure/SendUtils.h"
//#include "../../../infrastructure/VertexProxy.h"


// tvg_vertex_id dst_vid, tvg_long_counter mod_id, , );

class ReserveStructure
{
private:
    //
    const tvg_vertex_id m_lVertexID;
    const tvg_index m_lIndex;

    const tvg_time m_tEpoch;


public:
    //
    ReserveStructure(const tvg_vertex_id& lVertexID, const tvg_index& lIndex, const tvg_time& tEpoch) : m_lVertexID(lVertexID), m_lIndex(lIndex), m_tEpoch(tEpoch)
    {
    }


    const tvg_vertex_id& GetVertexID()
    {
        return m_lVertexID;
    }


    const tvg_index& GetIndex()
    {
        return m_lIndex;
    }


    const tvg_time& GetEpoch()
    {
        return m_tEpoch;
    }
};




class KHop_Input_Vertex
{
private:
    //
    tvg_vertex_id m_lID;
    tvg_counter m_iCurrDepth;

public:
    //
    KHop_Input_Vertex()
    {
        m_lID = -1;
        m_iCurrDepth = -1;
    }

    KHop_Input_Vertex(tvg_vertex_id lID, tvg_counter iCurrDepth)
    {
        m_lID = lID;
        m_iCurrDepth = iCurrDepth;
    }



};



class MyVertex
{
private:
    //

//    Vertica::ServerInterface _log_root;
    log4cpp::Category& _log_root;
    //
    //
    // Neighbors list
    //
    std::unique_ptr<AdjacencyDList> m_pDAdjacencyList;


    bool m_bNVCompleted;
    tvg_index m_lCurrentNVLocation;
    std::unique_ptr<NVRepository> m_pNVRepository;


//	static const double m_dPerThreshold;

    const tvg_vertex_id m_lID;
    tvg_time m_tCreationEpoch;


    tvg_long_counter m_lConnectionsCount;


    // Optimization
    //
    std::unique_ptr<AdjacentNVVertex> m_pLastAdded;


    bool m_bSubmitted;

 //   std::map<tvg_long_counter, std::unique_ptr<ReserveStructure>> m_mapModificationToData;

    std::map<tvg_long_counter, std::unique_ptr<SameNodeNVLink>> m_mapIdToSecondLink;


    bool m_bReleaseDRAM;

    bool m_bSearchMarked;

/*
	public static void SetTechnologieswFile(std::string strTechnologieswFile)
	{
		m_strTechnologieswFile = strTechnologieswFile;
	}
*/





    // private default constructor
    //
    MyVertex() : MyVertex(-1, 0) {};


    void ReleaseDRAM();



public:
    //
    static tvg_long_counter  m_lIncompleteSize;


    //
    //
    // QUERIES
    //
    // INSERT
    //
//    void UpdateConnection(tvg_vertex_id lID,  const std::unique_ptr<NVLink>& pThisNodeLink, const std::unique_ptr<NVLink>& pOtherNodeLink, const tvg_long_counter& lUID, const tvg_time& tEpoch, bool& bCompleteChain);
 //   void UpdateConnection(const tvg_long_counter& lUID,  std::unique_ptr<NVLink>, tvg_counter& iFirstCompleteCounter);
//    bool AddLocalConnection(std::unique_ptr<MyVertex>& pV2, const tvg_long_counter& lUID, tvg_time tEpoch, bool bSource, tvg_counter& iFirstCompleteCounter, tvg_counter& iSecondCompleteCounter);
//    void AddDistantConnection(std::unique_ptr<VertexProxy>& vp, const tvg_vertex_id& lID2, const tvg_long_counter& lUID, tvg_time tEpoch, tvg_vertex_id lNewNodeID, bool bSource, tvg_counter& iFirstCompleteCounter);

    tvg_counter AddConnection_Advanced(const tvg_long_counter& lUID, const tvg_vertex_id& lOtherVertexID, tvg_time tEpoch, bool bSource, MODIFICATION_TYPE m);




    // SELECT
    //
    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_Efficient(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch) const;
//    std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch,std::unique_ptr <SendUtils<> > sender) const;
  //  std::unique_ptr<std::vector<std::unique_ptr<NeighborMetaData>>> LoadIntervalNeighbors_from_db(const tvg_time& tStartEpoch, const tvg_time& tEndEpoch,std::unique_ptr <SendUtils<> > sender) const;




public:
    //
    MyVertex(tvg_vertex_id iID, tvg_time tEpoch);
    ~MyVertex();


    /*
    void IncreaseNeighboursCount() {m_lNeighboursCount++;}
    void DecreaseNeighboursCount() {m_lNeighboursCount--;}
    void SetNeighboursCount(long lNeighboursCount) {m_lNeighboursCount = lNeighboursCount;}
    long GetNeighboursCount() {return m_lNeighboursCount++;}
*/


/*
    std::unique_ptr<AdjacencyDList> GetDAdjacencyList()
    {
        return std::move(m_pDAdjacencyList);
    }
*/


    tvg_index Finalize(tvg_index& iChainCounter);


    void SetDoReleaseDRAM()
    {
        m_bReleaseDRAM = true;
    }

    bool GetDoReleaseDRAM()
    {
        return m_bReleaseDRAM;
    }





    const tvg_vertex_id GetID() const {return m_lID;}
    const tvg_time GetEpoch() {return m_tCreationEpoch;}


/*
    void AddToMapModificationToData(const tvg_long_counter& lUID, std::unique_ptr<ReserveStructure> pNewConnection)
    {
        m_mapModificationToData[lUID] = std::move(pNewConnection);
    }
*/


 //   int Compare(MyVertex* v);

    tvg_long_counter GetRealIndexToDSStructureSize()
    {
        return m_pDAdjacencyList->GetRealIndexToDSStructureSize();
    }

    tvg_long_counter GetRealSameValueToLastLocationSize()
    {
        return m_pDAdjacencyList->GetRealSameValueToLastLocationSize();
    }


    void SetIsSubmitted(bool bSubmitted)
    {
        m_bSubmitted = bSubmitted;
    }

    std::unique_ptr<AdjacentNVVertex[]> SubmitNVAdjacencyList()
    {
        if (!m_bSubmitted)
        {
            _log_root.debug("MyVertex::SubmitNVAdjacencyList: submitting, while the flag is not submitted.");
        }

        return std::move(m_pNVRepository->SubmitNVAdjacencyList());
    }


    std::unique_ptr<NVChainStructure[]> SubmitChains()
    {
        if (!m_bSubmitted)
        {
            _log_root.debug("MyVertex::SubmitChains: submitting, while the flag is not submitted.");
        }
        return std::move(m_pNVRepository->SubmitChains());
    }

    std::unique_ptr<NVChainStructure[]>& GetChains()
    {
        return m_pNVRepository->GetChains();
    }

    void ResetVertecesAndChains()
    {
        m_pNVRepository->ResetVertecesAndChains();
    }

    std::unique_ptr<SameNodeNVTemporalLink[]> SubmitAdditionalChains()
    {
        if (!m_bSubmitted)
        {
            _log_root.debug("MyVertex::SubmitAdditionalChains: submitting, while the flag is not submitted.");
        }

        return std::move(m_pNVRepository->SubmitAdditionalChains());
    }



    std::unique_ptr<NVLinkPairInterval[]> SubmitMapAdditionalNodesToChains()
    {
        if (!m_bSubmitted)
        {
            _log_root.debug("MyVertex::SubmitMetadata: submitting, while the flag is not submitted.");
        }

        return std::move(m_pNVRepository->SubmitMapAdditionalNodesToChains());
    }


    std::unique_ptr<tvg_index[]> SubmitMetadata()
    {
        if (!m_bSubmitted)
        {
            _log_root.debug("MyVertex::SubmitMetadata: submitting, while the flag is not submitted.");
        }



        return std::move(m_pNVRepository->SubmitMetadata());
    }





    std::unique_ptr<SameNodeNVLink> ShowCurrVertexLink(const tvg_long_counter& lUID);


    tvg_index GetCompletedVerticesElementsCount()
    {
        return m_pNVRepository->GetCompletedVerticesNVLocation();
    }

/*
    tvg_index GetCurrChainElementsCount()
    {
        return m_pNVRepository->GetCurrChainElementsCount();
    }
*/

    tvg_index GetCurrAdditionalChainElementsCount()
    {
        return m_pNVRepository->GetCurrAdditionalChainElementsCount();
    }


    tvg_index GetCurrMapAdditionalNodesToChainsElementsCount()
    {
        return m_pNVRepository->GetCurrMapAdditionalNodesToChainsElementsCount();
    }


    tvg_index GetCurrMetadataSize()
    {
        return m_pNVRepository->GetCurrMetadataSize();
    }




/////////////////////////////////////////////////


    /*
    public boolean equals(final Object obj)
    {
        if (obj == nullptr)
        {
            return false;
        }
        if (obj == this)
        {
            return true;
        }
        if (!(obj instanceof MyVertex))
        {
            return false;
        }
        MyVertex objClass = (MyVertex)obj;

        if (m_iID == objClass.m_iID)
            return true;

        return false;
    }

    @Override public int hashCode()
    {
        return (int) this.m_iID;
    }


    public std::string toString()
    {
        if (m_iID > 0)
            return "";
//			return Integer.toString(m_iID);

        if (m_technologiesData.IsSet())
            return m_technologiesData.GetVal(m_iID);
        else
            return "";
    }

    */



};



////////////////////////////////////////////////////////////////////////////////////////
//
class VertexDepth
{
private:
    //
    tvg_vertex_id m_lVertexID;
    int m_iDepth;

public:
    //
    VertexDepth(tvg_vertex_id lVertexID = -1, int lDepth = 0)
    {
        Init(lVertexID, lDepth);
    }

    void Init(tvg_vertex_id lVertexID, int lDepth = 0)
    {
        m_lVertexID = lVertexID;
        m_iDepth = lDepth;
    }


    const VertexDepth& operator=(const VertexDepth& other)
    {
        m_lVertexID = other.m_lVertexID;
        m_iDepth = other.m_iDepth;
        return *this;
    }


    const tvg_vertex_id GetVertexID() const
    {
        return m_lVertexID;
    }

    const int GetDepth() const
    {
        return m_iDepth;
    }
};


class CompareVertexDepth
{
public:
    //
    bool operator()(const VertexDepth& v1, const VertexDepth& v2)
    {
        if (v1.GetDepth() == v2.GetDepth())
        {
            if (v1.GetVertexID() > v2.GetVertexID())
                return false;
            else
                return true;
        }
        else
        {
            if (v1.GetDepth() > v2.GetDepth())
                return true;
            else
                return false;
        }
    }
};


#endif //TVG_IGOR_MYVERTEX_H
