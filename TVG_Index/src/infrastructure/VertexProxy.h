//
// Created by schein on 9/21/15.
//

#ifndef TVG4TM_VERTEXPROXY_H
#define TVG4TM_VERTEXPROXY_H
//#include <mpi.h>
#include <thread>

#include <log4cpp/Category.hh>
#include "../data_structures/General/Header/tvg_vertex_id.h"
#include "../../include/Globals.h"
//#include "TvgTask.h"
//#include "Uid.h"
//#include "Vertex2SoC.h"

class VertexProxy {
private :
    log4cpp::Category &_log_root;
    int _soc_id;
    bool _all_to_all_mode;
public:
    VertexProxy(int soc_id):_soc_id(soc_id),_log_root(log4cpp::Category::getRoot())
    {
//        int is_mpi_init;
//        MPI_Initialized(&is_mpi_init);
//        _is_mpi_init = is_mpi_init == 0 ? false : true;
//        _all_to_all_mode = Configuration::Instance()->GetIntegerItem("infrastructure.all-to-all") == 1 ? true : false;
        _all_to_all_mode = true;
    }


    int GetSocId() const
    {
        return _soc_id;
    }
    bool IsVertexLocal(tvg_vertex_id vid)
    {
        if(_all_to_all_mode)
            return true; //only when we have all files in place
        _log_root.debug("VertexProxy::IsVertexLocal [%i] start vid = %llu",_soc_id, (vid.Get()));
        int soc_id;
//        if(_is_mpi_init == false || _soc_id ==-1)
            return true; //this is a single app no mpi.

/*        bool is_mapped;

        std::tie(is_mapped,soc_id) = Vertex2SoC::Instance()->AlreadyMapped(vid);
        if(!is_mapped){
            soc_id = FindVertexSocLocation(vid);
            Vertex2SoC::Instance()->MapVertexToSoc(vid,soc_id);
        }


        return soc_id == _soc_id ? true : false;*/
    }
    void PushLink(tvg_vertex_id dst_vid,tvg_long_counter mod_id, tvg_vertex_id src_vid, tvg_index link_idx, tvg_time tEpoch)
    {

//        if(_is_mpi_init ==false || _soc_id == -1){
            _log_root.debug("VertexProxy::PushLink no need to push edge on a single process application ");
            return;
//        }
/*
        int soc_id;
        bool is_mapped;
        std::tie(is_mapped,soc_id) = Vertex2SoC::Instance()->AlreadyMapped(dst_vid);
        if(!is_mapped) {
            soc_id = FindVertexSocLocation(dst_vid);
            Vertex2SoC::Instance()->MapVertexToSoc(dst_vid,soc_id);
        }

        TvgPushLink pl(link_idx,src_vid,dst_vid,tEpoch);
        pl.SetTid(mod_id);

        _log_root.warn("VertexProxy::PushLink ["+std::to_string(_soc_id)+"] push updated link = ("+std::to_string(src_vid)+","+std::to_string(dst_vid)+")"+
                       " to soc "+std::to_string(soc_id));
        int err = MPI_Send((unsigned char *) &pl, sizeof(TvgPushLink), MPI_BYTE, soc_id, static_cast<int>(TVG_COMM_TYPE::NORMAL),MPI_COMM_WORLD);
        if (err != MPI_SUCCESS) {
            _log_root.error("VertexProxy::PushLink failed. MPI_Send got " + std::to_string(err));
        }
*/

    }




private:

    VertexProxy(const VertexProxy &) = delete;
    const VertexProxy & operator=(const VertexProxy &) = delete;

    int FindVertexSocLocation(tvg_vertex_id vid)
    {
        _log_root.debug("VertexProxy::FindVertexSocLocation [%i] Start ",_soc_id);
        int soc_id;
//        constexpr int needed_buf_sz = ConstMax(ConstMax(sizeof(TvgInsert),sizeof(TvgSelectResult)), sizeof(TvgSelect));
        bool is_mapped;
//        unsigned char msg_buffer[needed_buf_sz];
        int master_processor = 0;
//        if(_is_mpi_init ==false)//if there is not MPI this is a non-mpi application - reside on a single machine (atleast until we go away from mpi)
            return _soc_id;
/*
        std::tie(is_mapped,soc_id) = Vertex2SoC::Instance()->AlreadyMapped(vid);
        if(is_mapped)
            return soc_id;

        TvgQuerySocId query_soc_id(vid);
        query_soc_id.SetTid(Uid::Instance()->Get());
        query_soc_id.SetSoC(_soc_id);
        query_soc_id.SetTaskSubmitTimeNow();
        int err = MPI_Send((unsigned char *) &query_soc_id, sizeof(TvgQuerySocId), MPI_BYTE, master_processor, static_cast<int>(TVG_COMM_TYPE::DETOUR),MPI_COMM_WORLD);
        if (err != MPI_SUCCESS) {
            _log_root.error("VertexProxy::FindVertexSocLocation failed MPI_Send got " + std::to_string(err));
        }

        //recieve response
        MPI_Status status;
        int msg_size, msg_size_keep;
        MPI_Probe(master_processor, static_cast<int>(TVG_COMM_TYPE::DETOUR), MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_BYTE, &msg_size);
        //this is super odd - it seems that the  call to MPI_Recv zeros the msg_size variable.
        //this can happen if the API moves reference !! I keep this for later inspection as it is really odd
        //alternative explenation : there is a second thread calling the same event loop. Need to check this
        msg_size_keep = msg_size;
        //////////// SAGI !!!!!!!!!!
        //continue;

        MPI_Recv(msg_buffer, msg_size, MPI_BYTE, master_processor, static_cast<int>(TVG_COMM_TYPE::DETOUR), MPI_COMM_WORLD, &status);

        //_log_root.warn("Vertexproxy::FindVertexSocLocation sz="+std::to_string(msg_size_keep)+" expected "+std::to_string(sizeof(TvgQuerySocId))+
        //               " type "+std::to_string(int(reinterpret_cast<TvgQuerySocId *>(msg_buffer)->type()))+ " expected "+std::to_string(int(TaskType::QUERY_SOC)));
        int ret_soc_id = -1;
        tvg_vertex_id ret_vid = -1;
        if (msg_size == sizeof(TvgQuerySocId) &&
            reinterpret_cast<TvgQuerySocId *>(msg_buffer)->type() == TaskType::QUERY_SOC) {
            ret_soc_id = reinterpret_cast<TvgQuerySocId *>(msg_buffer)->GetSocId();
            ret_vid = reinterpret_cast<TvgQuerySocId *>(msg_buffer)->GetVid();
            _log_root.warn("Vertexproxy::FindVertexSocLocation got QUERY_SOC response "+std::to_string(ret_vid)+" : "+std::to_string(ret_soc_id));
            */
/*
            //Vertex2SoC::Instance()->MapVertexToSoc(ret_vid,ret_soc_id);*//*

        }
        else{
            _log_root.warn("Vertexproxy::FindVertexSocLocation got weird message");
        }
        _log_root.debug("VertexProxy::FindVertexSocLocation End, vid "+std::to_string(ret_vid)+" in soc "+std::to_string(ret_soc_id));
        return ret_soc_id;//Vertex2SoC::Instance()->GetSocId(vid);
*/
    }


};




#endif //TVG4TM_VERTEXPROXY_H
