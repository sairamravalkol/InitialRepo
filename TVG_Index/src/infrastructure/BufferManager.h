//
// Created by norig on 6/29/15.
//

#ifndef TVG_IGOR_BUFFERMANAGER_H
#define TVG_IGOR_BUFFERMANAGER_H
//#include "TargetNVM.h"
//#include "TargetTextFile.h"
#include "../data_structures/NVRAM/Header/NVRepository.h"
#include "../data_structures/NVRAM/Header/AdjacentNVStructure.h"
#include "../data_structures/NVRAM/Header/AdjacentNVVertex.h"
#include "../data_structures/NVRAM/Header/NVChainStructure.h"


class AdjacentNVVertex;
class NVChainStructure;


template <class TargetBackend> class BufferManager
{

public:

    static BufferManager<TargetBackend> * Instance()
    {

        static BufferManager<TargetBackend>* global = new BufferManager<TargetBackend>();
        return global;
    }

    void ClearAllBuffers()
    {
        _backend.ClearAllBuffers();
    }

    std::tuple<int, tvg_long_counter,tvg_long_counter,tvg_long_counter> HeapUsage()
    {
        return _backend.HeapUsage();
    }

    void LoadVertex2FileMapping()
    {
        _backend.LoadVertex2FileMapping();
    }
    // Submit
    //
    tvg_long_counter Submit(int vertex_id,std::unique_ptr<AdjacentNVVertex[]> vertex_buffer, tvg_long_counter num_items)
    {
        return _backend.Submit(vertex_id,std::move(vertex_buffer),num_items);
    }

    tvg_long_counter Submit(int vertex_id,std::unique_ptr<NVChainStructure[]> vertex_buffer, tvg_long_counter num_items)
    {
        return _backend.Submit(vertex_id,std::move(vertex_buffer),num_items);
    }

    tvg_long_counter Submit(int vertex_id,std::unique_ptr<SameNodeNVTemporalLink[]> vertex_buffer, tvg_long_counter num_items)
    {
        return _backend.Submit(vertex_id,std::move(vertex_buffer),num_items);
    }

    tvg_long_counter Submit(int vertex_id,std::unique_ptr<NVLinkPairInterval[]> vertex_buffer, tvg_long_counter num_items)
    {
        return _backend.Submit(vertex_id,std::move(vertex_buffer),num_items);
    }

    tvg_long_counter Submit(int vertex_id,std::unique_ptr<tvg_index[]> vertex_buffer, tvg_long_counter num_items)
    {
        return _backend.Submit(vertex_id,std::move(vertex_buffer),num_items);
    }

    tvg_long_counter Submit(int vertex_id,std::unique_ptr<tvg_vertex_id[]> vertex_buffer, tvg_long_counter num_items)
    {
        return _backend.Submit(vertex_id,std::move(vertex_buffer),num_items);
    }


    template<class T> tvg_long_counter SubmitGenericVector(const std::string& vector_name, std::unique_ptr<T[]> metadata_vector, tvg_long_counter num_items)
    {
        return _backend.SubmitGenericVector(vector_name, std::move(metadata_vector), num_items);
    }


    template<class T> std::pair<tvg_long_counter,std::unique_ptr<T[]>> GetGenericVector(const std::string& vector_name)
    {
        return _backend.GetGenericVector<T>(vector_name);
    }



    template<class T> bool FixSubmitVertex(tvg_vertex_id vertex_id, tvg_long_counter offset, std::unique_ptr<T> value)
    {
        return _backend.FixSubmitVertex(vertex_id, offset, std::move(value));
    }


    // Get
    //
    template<class T> std::unique_ptr<T[]> Get(int vertex_id, int offset, int num_items)
    {
        return std::move(_backend.Get<T>(vertex_id, offset, num_items));
    }





/*
    unsigned long Submit(int vertex_id,AdjacentNVVertex* vertex_buffer,int num_items)
    {
        auto ptr = std::unique_ptr<AdjacentNVVertex[]>(vertex_buffer);
        auto v = Submit(vertex_id,std::move(vertex_buffer),num_items);
        ptr.release();
        return v;
    }
    unsigned long Submit(int vertex_id,NVChainStructure* vertex_chain,int num_items)
    {
        auto ptr = std::unique_ptr<NVChainStructure[]>(vertex_chain);
        auto v = Submit(vertex_id,std::move(vertex_chain),num_items);
        ptr.release();//SAGI -- this is a memory leak . release does not relases the memory in a unique_ptr it simply release the responsibility
        return v;
    }
*/
    std::pair<unsigned long,unsigned long> NumCopiedItems(int vertex_id)
    {
        return _backend.NumCopiedItems();
    }




private:

    BufferManager(){
        log4cpp::Category::getRoot().debug("BufferManager");
    }
    ~BufferManager(){}


    BufferManager(const BufferManager&)=delete;
    const BufferManager& operator=(const BufferManager&)=delete;


    TargetBackend _backend;

};


#endif //TVG_IGOR_BUFFERMANAGER_H
