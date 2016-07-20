//
// Created by norig on 11/5/15.
//

#ifndef TVG4TM_DEBUGINFO_H
#define TVG4TM_DEBUGINFO_H


#include <string>
#include <fstream>


#include <boost/filesystem/operations.hpp>
#include <set>

#include "../../General/Header/tvg_time.h"
#include "../../../../include/Globals.h"

#include "../../NVRAM/Header/NVRepository.h"
#include "../../NVRAM/Header/AdjacentNVStructure.h"
#include "../../NVRAM/Header/AdjacentNVVertex.h"
#include "../../NVRAM/Header/NVChainStructure.h"


class DebugInfo
{
private:
    //
    static int m_iExecCounter;

    static int m_iCounterVertice;
    static int m_iCounterChain;
    static int m_iCounterAdditionalChain;
    static int m_iCounterAdditionalPairsChain;
    static int m_iCounterMetadata;

    std::vector<tvg_vertex_id> m_vVertecesToPrint;
    bool m_bPrintData;


    std::string m_strName;
    std::ofstream m_file;


    std::string m_strVerticesDirectory;
    std::string m_strChainDirectory;
    std::string m_strAdditionalChainDirectory;
    std::string m_strAdditionalPairsChainDirectory;
    std::string m_strMetadataDirectory;

    const std::string m_strRootOriginal;
    std::string m_strRoot_Curr;




private:
    //
    DebugInfo() : m_strRootOriginal("DebugInfo/")
    {
        try {
            boost::filesystem::remove_all(m_strRootOriginal);
            boost::filesystem::create_directory(m_strRootOriginal);

            m_strRoot_Curr = m_strRootOriginal + std::to_string(m_iExecCounter) + "/";
            boost::filesystem::create_directory(m_strRoot_Curr);
        }catch(...) {
            std::cout<<"failed to construct dirstructure for DebugInfo"<<std::endl;
        }

        m_bPrintData = false;
    }

    ~DebugInfo(){}




public:
    //
    static DebugInfo* Instance()
    {
        static DebugInfo* global = new DebugInfo();
        return global;
    }


    bool IsPrintData()
    {
        return m_bPrintData;
    }


    void ResetCounters()
    {
        m_iExecCounter++;
        m_strRoot_Curr = m_strRootOriginal + std::to_string(m_iExecCounter) + "/";
        boost::filesystem::create_directory(m_strRoot_Curr);

        m_iCounterVertice = -1;
        m_iCounterChain = -1;
        m_iCounterAdditionalChain = -1;
        m_iCounterAdditionalPairsChain = -1;
        m_iCounterMetadata = -1;
    }

    void SetPrintData(bool bPrintData)
    {
        m_bPrintData = bPrintData;
        m_vVertecesToPrint.clear();

    }

    void SetVertecesToPrint(std::vector<tvg_vertex_id>& vVertecesToPrint)
    {
        for (auto& v : vVertecesToPrint)
        {
            m_vVertecesToPrint.push_back(v);
        }

        m_bPrintData = true;
    }

    void SetVertecesToPrint(std::set<tvg_vertex_id>& vVertecesToPrint)
    {
        for (auto& v : vVertecesToPrint)
        {
            m_vVertecesToPrint.push_back(v);
        }

        m_bPrintData = true;
    }



    void ResetSetVertecesToPrint()
    {
        m_vVertecesToPrint.clear();
        m_bPrintData = false;
    }

    bool GetPrintData()
    {
        return m_bPrintData;
    }


    void Print(tvg_vertex_id vertex_id, std::unique_ptr<AdjacentNVVertex[]>& vertex_buffer, tvg_index num_items)
    {
        if (!m_bPrintData)
            return;

        if ((!m_vVertecesToPrint.empty()) && (std::find(m_vVertecesToPrint.begin(), m_vVertecesToPrint.end(), vertex_id) == m_vVertecesToPrint.end()))
            return;


        if (num_items <= tvg_index((0)))
            return;

        m_iCounterVertice++;
        m_strVerticesDirectory = m_strRoot_Curr + "Vertices_" + std::to_string(m_iCounterVertice) + "/";
        m_strVerticesDirectory = m_strRoot_Curr + "Vertices/";

        if (!(boost::filesystem::exists(m_strVerticesDirectory)))
            boost::filesystem::create_directory(m_strVerticesDirectory);


//    m_strName = Configuration::Instance()->GetStringItem("data-structure.filename-adjacency-list") + std::to_string(vertex_id) + ".txt";

        m_strName = m_strVerticesDirectory + "Vertex_" + std::to_string(vertex_id) + ".txt";


        // To remove the previous version
        //
//        m_file.open(m_strName, std::ios::out);
//        m_file.close();
 //       std::remove(m_strName.c_str());



        m_file.open(m_strName, std::ios::out | std::ios::app);

//    AdjacentNVVertex* pArray = vertex_buffer.get();
        for (int i = 0; i < num_items; i++)
        {
            vertex_buffer[i].Submit(m_file);
        }


        m_file.close();
    }




    void Print(tvg_vertex_id vertex_id, std::unique_ptr<NVChainStructure[]>& chain_buffer, tvg_index num_items)
    {
        if (!m_bPrintData)
            return;

        if (num_items <= tvg_index((0)))
            return;

        if ((!m_vVertecesToPrint.empty()) && (std::find(m_vVertecesToPrint.begin(), m_vVertecesToPrint.end(), vertex_id) == m_vVertecesToPrint.end()))
            return;


        m_iCounterChain++;
        m_strChainDirectory = m_strRoot_Curr + "Chain_" + std::to_string(m_iCounterChain) + "/";
        m_strChainDirectory = m_strRoot_Curr + "Chain/";

        if (!(boost::filesystem::exists(m_strChainDirectory)))
           boost::filesystem::create_directory(m_strChainDirectory);



        tvg_long_counter lRes = 0;

//    m_strName = Configuration::Instance()->GetStringItem("data-structure.filename-chain") +  std::to_string(vertex_id) + ".txt";



        m_strName = m_strChainDirectory + "Chain_" + std::to_string(vertex_id) + ".txt";


        // To remove the previous version
        //
//        m_file.open(m_strName, std::ios::out);
//        m_file.close();
//        std::remove(m_strName.c_str());




        m_file.open(m_strName, std::ios::out | std::ios::app);

        //   NVChainStructure* pArray = chain_buffer.get();
        for (int i = 0; i < num_items; i++)
        {
            lRes += chain_buffer[i].Submit(m_file, i);
        }


        m_file.close();
    }





    void Print(tvg_vertex_id vertex_id, std::unique_ptr<SameNodeNVTemporalLink[]>& additional_chain_buffer, tvg_index num_items)
    {
        if (!m_bPrintData)
            return;


        if (num_items <= tvg_index((0)))
            return;

        if ((!m_vVertecesToPrint.empty()) && (std::find(m_vVertecesToPrint.begin(), m_vVertecesToPrint.end(), vertex_id) == m_vVertecesToPrint.end()))
            return;


        m_iCounterAdditionalChain++;
        m_strAdditionalChainDirectory = m_strRoot_Curr + "AdditionalChain_" + std::to_string(m_iCounterAdditionalChain) + "/";
        m_strAdditionalChainDirectory = m_strRoot_Curr + "AdditionalChain/";


        if (!(boost::filesystem::exists(m_strAdditionalChainDirectory)))
            boost::filesystem::create_directory(m_strAdditionalChainDirectory);






        tvg_long_counter lRes = 0;

//    m_strName = Configuration::Instance()->GetStringItem("data-structure.filename-chain") +  std::to_string(vertex_id) + ".txt";



        m_strName = m_strAdditionalChainDirectory + "AdditionalChain_" + std::to_string(vertex_id) + ".txt";


        // To remove the previous version
        //
//        m_file.open(m_strName, std::ios::out);
//        m_file.close();
 //       std::remove(m_strName.c_str());




        m_file.open(m_strName, std::ios::out | std::ios::app);

        //   NVChainStructure* pArray = chain_buffer.get();
        for (int i = 0; i < num_items; i++)
        {
            lRes += additional_chain_buffer[i].Submit(m_file);
        }


        m_file.close();
    }



    void Print(tvg_vertex_id vertex_id, std::unique_ptr<NVLinkPairInterval[]>& additional_chain_pair_interval_buffer, tvg_index num_items)
    {
        if (!m_bPrintData)
            return;

        if (num_items <= tvg_index((0)))
            return;

        if ((!m_vVertecesToPrint.empty()) && (std::find(m_vVertecesToPrint.begin(), m_vVertecesToPrint.end(), vertex_id) == m_vVertecesToPrint.end()))
            return;


        m_iCounterAdditionalPairsChain++;
        m_strAdditionalPairsChainDirectory = m_strRoot_Curr + "AdditionalPairsChain_" + std::to_string(m_iCounterAdditionalPairsChain) + "/";
        m_strAdditionalPairsChainDirectory = m_strRoot_Curr + "AdditionalPairsChain/";


        if (!(boost::filesystem::exists(m_strAdditionalPairsChainDirectory)))
            boost::filesystem::create_directory(m_strAdditionalPairsChainDirectory);





        tvg_long_counter lRes = 0;

//    m_strName = Configuration::Instance()->GetStringItem("data-structure.filename-chain") +  std::to_string(vertex_id) + ".txt";



        m_strName = m_strAdditionalPairsChainDirectory + "AdditionalChain_Map_" + std::to_string(vertex_id) + ".txt";


        // To remove the previous version
        //
//        m_file.open(m_strName, std::ios::out);
//        m_file.close();
//        std::remove(m_strName.c_str());





        m_file.open(m_strName, std::ios::out | std::ios::app);

        //   NVChainStructure* pArray = chain_buffer.get();
        for (int i = 0; i < num_items; i++)
        {
            lRes += additional_chain_pair_interval_buffer[i].Submit(m_file);
        }


        m_file.close();
    }


    void Print(tvg_vertex_id vertex_id, std::unique_ptr<tvg_index[]>& metadata_buffer, tvg_index num_items)
    {
        if (!m_bPrintData)
            return;


        if (num_items <= tvg_index((0)))
            return;


        m_iCounterMetadata++;
        m_strMetadataDirectory = m_strRoot_Curr + "Metadata_" + std::to_string(m_iCounterMetadata) + "/";
        m_strMetadataDirectory = m_strRoot_Curr + "Metadata/";


        if (!(boost::filesystem::exists(m_strMetadataDirectory)))
            boost::filesystem::create_directory(m_strMetadataDirectory);


        tvg_long_counter lRes = 0;

//    m_strName = Configuration::Instance()->GetStringItem("data-structure.filename-chain") +  std::to_string(vertex_id) + ".txt";



        m_strName = m_strMetadataDirectory + "Metadata_" + std::to_string(vertex_id) + ".txt";


        // To remove the previous version
        //
//        m_file.open(m_strName, std::ios::out);
//        std::remove(m_strName.c_str());




        m_file.open(m_strName, std::ios::out | std::ios::app);

        //   NVChainStructure* pArray = chain_buffer.get();

        m_file<<"---------------------------"<<std::endl;
        m_file<<"m_lSavedVertecesCurrCompletedSize = "<<metadata_buffer[0]<<std::endl;
        m_file<<"m_lSavedVertecesRemovedSize = "<<metadata_buffer[1]<<std::endl;
        m_file<<"m_lSavedChainRemovedSize = "<<metadata_buffer[2]<<std::endl;
        m_file<<"m_lSavedChainCurrSize = "<<metadata_buffer[3]<<std::endl;
        m_file<<"m_lSavedAdditionalChainRemovedLength = "<<metadata_buffer[4]<<std::endl;
        m_file<<"m_lSavedAdditionalChainCurrLength = "<<metadata_buffer[5]<<std::endl ;
        m_file<<std::endl<<std::endl;

 //       lRes = sizeof(tvg_index) * num_items;
        m_file.close();
    }


    void Print(tvg_vertex_id vertex_id, std::unique_ptr<tvg_vertex_id[]>& metadata_buffer, tvg_index num_items)
    {
        if (!m_bPrintData)
            return;


        if (num_items <= tvg_index((0)))
            return;

        if ((!m_vVertecesToPrint.empty()) && (std::find(m_vVertecesToPrint.begin(), m_vVertecesToPrint.end(), vertex_id) == m_vVertecesToPrint.end()))
            return;

        m_strName = m_strRoot_Curr + "GlobalGraphData.txt";


        // To remove the previous version
        //
//        m_file.open(m_strName, std::ios::out);
 //       std::remove(m_strName.c_str());




        m_file.open(m_strName, std::ios::out | std::ios::app);

        //   NVChainStructure* pArray = chain_buffer.get();

        for (int i = 0 ; i < num_items ; i++)
        {
            m_file<<"---------------------------"<<std::endl;
            m_file<<"NodeID = "<<metadata_buffer[i]<<std::endl;
        }


        m_file.close();
    }



    void PrintVector(std::unique_ptr<tvg_vertex_id[]>& metadata_vector, unsigned long num_items)
    {
        if (!m_bPrintData)
            return;


        if (num_items <= tvg_index((0)))
            return;


        m_strName = m_strRoot_Curr + "GlobalGraphData.txt";


        // To remove the previous version
        //
//        m_file.open(m_strName, std::ios::out);
        //       std::remove(m_strName.c_str());




        m_file.open(m_strName, std::ios::out | std::ios::app);

        //   NVChainStructure* pArray = chain_buffer.get();

        for (int i = 0 ; i < num_items ; i++)
        {
            m_file<<"---------------------------"<<std::endl;
            m_file<<"NodeID = "<<metadata_vector[i]<<std::endl;
        }


        m_file.close();

    }



    void PrintVector(std::unique_ptr<std::pair<tvg_vertex_id,int>[]>& metadata_vector, unsigned long num_items)
    {

    }



    void PrintFixSubmit(tvg_vertex_id& vertex_id, int offset, std::unique_ptr<NVChainStructure>& value)
    {
        if (!m_bPrintData)
            return;

        if (value == nullptr)
            return;

        if ((!m_vVertecesToPrint.empty()) && (std::find(m_vVertecesToPrint.begin(), m_vVertecesToPrint.end(), vertex_id) == m_vVertecesToPrint.end()))
            return;


//        m_iCounterChain++;
        m_strChainDirectory = m_strRoot_Curr + "Chain/";

        if (!(boost::filesystem::exists(m_strChainDirectory)))
            boost::filesystem::create_directory(m_strChainDirectory);




        m_strName = m_strChainDirectory + "Chain_" + std::to_string(vertex_id) + ".txt";


        // To remove the previous version
        //
//        m_file.open(m_strName, std::ios::out);
//        m_file.close();
//        std::remove(m_strName.c_str());




        m_file.open(m_strName, std::ios::out | std::ios::app);

        value->Submit(m_file, offset);
        m_file<<"PrintFixSubmit"<<std::endl;


        m_file.close();
    }


    void PrintFixSubmit(tvg_vertex_id& vertex_id, int offset, std::unique_ptr<AdjacentNVVertex>& value)
    {


    }
};


#endif //TVG4TM_DEBUGINFO_H
