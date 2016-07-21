//
// Created by schein on 8/3/15.
//

#ifndef TVG4TM_TARGETNVM_H
#define TVG4TM_TARGETNVM_H
#include <unistd.h>
#include <sys/types.h>
#include <memory>
#include <map>
#include <mutex>
#include <atomic>
#include <queue>
#include <chrono>
#include <boost/interprocess/managed_mapped_file.hpp>
#include <boost/filesystem.hpp>
//#include <log4cpp/Category.hh>
#include "../data_structures/NVRAM/Header/NVRepository.h"
#include "../data_structures/NVRAM/Header/AdjacentNVStructure.h"
#include "../data_structures/NVRAM/Header/AdjacentNVVertex.h"
#include "../data_structures/Graph/Header/DebugInfo.h"



namespace bip = boost::interprocess;

class TargetNVM
{
    //
    std::map<std::pair<tvg_vertex_id,short>,int> _items_per_vertex; //(first = AdjacentNVVertex[], NVChainStructure);

    std::map<short,int> _tagToSize;
    log4cpp::Category&_log_root;
    int _soc_id;
    std::mutex _lock;
    std::string _dir_path;
    int _num_submit;
    int _num_bit;
    double _avg_submit;

    std::string _region_prefix;
    std::map<int, bip::managed_mapped_file> _nvm_region_map;
    std::map<unsigned long long, int> _vertex_to_file_mapping;
    std::map<std::string,std::pair<std::chrono::high_resolution_clock::time_point, void *>> _cache;//std::chrono::high_resolution_clock
    std::list<std::string> _fifo;
    bool _all_to_all_mode;
    int _fifo_size;


public:

    TargetNVM():_log_root(log4cpp::Category::getRoot()),_num_submit(0),_avg_submit(0),_region_prefix("nvm_region"),_fifo_size(100)
    {
        _log_root.debug("TargetNVM:TargetNVM");

        std::lock_guard<std::mutex> lock(_lock);
//        _all_to_all_mode = Configuration::Instance()->GetIntegerItem("infrastructure.all-to-all") == 1 ? true : false;
        _all_to_all_mode = true;
//        _soc_id = Configuration::Instance()->GetIntegerItem("infrastructure.soc-id");
        _soc_id = -1;
        _num_bit = 30;
        //_dir_path=std::string("/dev/shm/")+std::to_string(getuid());
        _dir_path=std::string("/tmp/")+std::to_string(getuid());
        boost::filesystem::path dir_path(_dir_path);
        if(!boost::filesystem::exists(dir_path)){
            if (boost::filesystem::create_directory(dir_path) != true)
                log4cpp::Category::getRoot().error("TargetNVM - failed to create target directory "+dir_path.string());
        }
        else if(!boost::filesystem::is_directory(dir_path)){
            log4cpp::Category::getRoot().error("TargetNVM - failed to verify directory "+dir_path.string());
        }

        std::string path = NvmRegionName();
        _log_root.debug("NvmRegionName: " + path);
        //std::remove(path.c_str());//make sure we do not already have -not anymore we need to save those
        {
            bip::managed_mapped_file(bip::open_or_create, path.c_str(),1<<_num_bit);//define a 1GB file for backing
        }
        //_nvm_stationary_region = bip::managed_mapped_file(bip::open_or_create, path.c_str()+std::string(".static"),1<<_num_bit);//define a 1GB file for backing

        if(_all_to_all_mode) {
            for (auto itr = boost::filesystem::directory_iterator(dir_path);
                 itr != boost::filesystem::directory_iterator(); itr++) {

                auto region_name = std::string((itr)->path().filename().c_str());

                auto s = region_name.find(_region_prefix);
                if (s == std::string::npos) {
                    continue;
                }
                auto e = region_name.find(".");
                auto file_id = std::stoi(
                        region_name.substr(s + _region_prefix.length(), e - (s + _region_prefix.length())));
//
//                if (file_id < 0)
//                    continue;
                //log4cpp::Category::getRoot().error(std::string("TargetNVM["+std::to_string(_soc_id)+"] open file id "+std::to_string(file_id))+" "+(itr)->path().c_str());
                _nvm_region_map.insert(std::make_pair(file_id, bip::managed_mapped_file(bip::open_or_create,
                                                                                        (itr)->path().c_str(),
                                                                                        1 << _num_bit)));
                //log4cpp::Category::getRoot().error("TargetNVM["+std::to_string(_soc_id)+"] -  mapped done for file_id "+std::to_string(file_id));

            }
        }
        else{
            _nvm_region_map.insert(std::make_pair(_soc_id, bip::managed_mapped_file(bip::open_or_create,
                                                                                    path.c_str(),
                                                                                    1 << _num_bit)));
        }
        //_nvm_region = _nvm_region_map[_soc_id];
        LoadVertex2FileMapping();


        log4cpp::Category::getRoot().error(std::string("TargetNVM["+std::to_string(_soc_id)+"] construction done"));
    }

    ~TargetNVM()
    {
        //ClearAllBuffers();//we do not want to clear this as we move to two separate programs
//        auto stat = HeapUsage();
//        log4cpp::Category::getRoot().info("TargetNVM - "+std::to_string(_soc_id)+" NVM heap report - heap_size:"+
//                                                  std::to_string(std::get<0>(stat))+" allocated:"+std::to_string(std::get<1>(stat))+
//                                                  " used:"+std::to_string(std::get<2>(stat)));
    }

    void LoadVertex2FileMapping()
    {
        //log4cpp::Category::getRoot().error("TargetNVM["+std::to_string(_soc_id)+"] -  LoadVertex2FileMapping ");
        for(auto & nvm_region : _nvm_region_map) {
            auto file_id = nvm_region.first;
            auto to_deserialize = GetGenericVector<std::pair<tvg_vertex_id, int>>(file_id, std::string("_vertex_to_soc_mapping"));

            //log4cpp::Category::getRoot().error(std::string("TargetNVM["+std::to_string(_soc_id)+"] read ")+std::to_string(to_deserialize.first)+" items from file "+std::to_string(file_id));
            for (int i = 0; i < to_deserialize.first; i++) {
                _vertex_to_file_mapping.insert(to_deserialize.second[i]);
                //log4cpp::Category::getRoot().error(std::string("TargetNVM ("+std::to_string(_soc_id)+") ")+std::to_string(to_deserialize.second[i].first)+":"+std::to_string(to_deserialize.second[i].second));
            }
        }

    }

    bool WriteToDevShm()
    {
        return (_dir_path.find("/dev/shm") == std::string::npos) ? false : true;
    }

    void ClearAllBuffers()
    {

        RemoveStoredNVMFiles();
        {
            std::lock_guard<std::mutex> lock(_lock);
            std::string path = NvmRegionName();
            _nvm_region_map[_soc_id] = bip::managed_mapped_file(bip::open_or_create, path.c_str(),
                                                   1 << _num_bit);//define a 1GB file for backing
            //_nvm_stationary_region = bip::managed_mapped_file(bip::open_or_create, path.c_str()+std::string(".static"),1<<_num_bit);
        }

    }

    std::tuple<int, tvg_long_counter,tvg_long_counter,tvg_long_counter> HeapUsage()
    {
        auto cur_heap_size = boost::filesystem::file_size(NvmRegionName());
        unsigned long long heap_size = 0;
        unsigned long long allocated_size = 0;
        unsigned long long used_size = 0;
        for(auto item :_items_per_vertex){

            //allocated_size+=bip::managed_mapped_file::get_instance_length((std::to_string(item.first.first) + std::to_string(item.first.second)).c_str());
            auto obj_in_bytes = _tagToSize[item.first.second];
            used_size+=item.second*obj_in_bytes;
        }
        allocated_size = _nvm_region_map[_soc_id].get_size() - _nvm_region_map[_soc_id].get_free_memory();

        return std::make_tuple(_soc_id,cur_heap_size,allocated_size,used_size);
    }

    void RemoveStoredNVMFiles()
    {
        std::lock_guard<std::mutex> lock(_lock);
        _nvm_region_map[_soc_id].flush();
        _items_per_vertex.clear();

        std::string path = NvmRegionName();
        std::remove(path.c_str());

    }


//    template<class T> std::unique_ptr<T[]> Get(tvg_vertex_id vertex_id, int offset, int num_items)
//    {
//
//        try {
//
//            auto buffer = std::make_unique<T[]>(num_items);
//            auto vertex_name = VertexName<T>(vertex_id);
////            _log_root.warn("TargetNVM::Get["+std::to_string(_soc_id)+std::string("] for vertex id ")+vertex_name+
////                           " offset "+std::to_string(offset) + " num_items "+std::to_string(num_items));
//            auto mapped = _nvm_region.find<T>(vertex_name.c_str());
////            _log_root.warn("TargetNVM::Get["+std::to_string(_soc_id)+"] for vertex id "+std::to_string(vertex_id)+
////                           " offset "+std::to_string(offset) + " num_items "+std::to_string(num_items)+" mapped success");
//            if(mapped.second == 0) {
//                _log_root.error("TargetNVM::Get["+std::to_string(_soc_id)+"] failed to find vertex "
//                               + std::to_string(vertex_id) + " in soc " + std::to_string(_soc_id));
//                throw std::out_of_range(
//                        "failed to find vertex " + std::to_string(vertex_id) + " in soc " + std::to_string(_soc_id));
//            }
//            else if(mapped.second < offset+num_items) {
//                _log_root.error("TargetNVM::Get["+std::to_string(_soc_id)+"] invalid array offset "+
//                                        std::to_string(offset+num_items)+
//                                        " for vertex "+std::to_string(vertex_id)+
//                                        " in soc "+std::to_string(_soc_id)+" that have "+
//                                        std::to_string(mapped.second)+" items in nvm");
//                throw std::out_of_range("invalid array offset "+std::to_string(offset+num_items)+
//                                                " for vertex "+std::to_string(vertex_id)+" in soc "+std::to_string(_soc_id)+" that have "+
//                                                std::to_string(mapped.second)+" items in nvm");
//            }
////            _log_root.warn("TargetNVM::Get["+std::to_string(_soc_id)+"] for vertex id "+std::to_string(vertex_id)+
////                           " offset "+std::to_string(offset) + " num_items "+std::to_string(num_items)+" array size = "+
////                           std::to_string(mapped.second));
//            memcpy(buffer.get(),mapped.first+offset,num_items*sizeof(T));
//
//            return buffer;
//        }
//        catch(std::out_of_range e){
//            _log_root.error("TargetNVM::Get ["+std::to_string(_soc_id)+"] - failed to get buffer - "+std::string(e.what()));
//            throw std::out_of_range("failed to find vertex "+std::to_string(vertex_id)+" with type "+ typeid(T).name()+" tag "+std::to_string(ClassTypeToUniqueChar<T>())+" in soc "+std::to_string(_soc_id));
//        }
//        catch(...)
//        {
//            _log_root.error("TargetNVM::Get ["+std::to_string(_soc_id)+"] - failed to get buffer from NVM for vid "+std::to_string(vertex_id));
//            throw;
//        }
//
//    }

    template<class T> std::unique_ptr<T[]> Get(int file_id, tvg_vertex_id vertex_id, int offset, int num_items)
    {
        //_log_root.warn("TargetNVM["+std::to_string(_soc_id)+"]::<"+typeid(T).name()+">Get Start id "+std::to_string(vertex_id)+" file "+std::to_string(file_id));


        try {

            auto buffer = std::make_unique<T[]>(num_items);
            auto vertex_name = VertexName<T>(vertex_id);
            auto cached = _cache.find(vertex_name);
            if(cached == _cache.end()){


    //            _log_root.warn("TargetNVM::Get["+std::to_string(_soc_id)+std::string("] for vertex id ")+vertex_name+
    //                           " offset "+std::to_string(offset) + " num_items "+std::to_string(num_items));
                auto mapped = _nvm_region_map[file_id].find<T>(vertex_name.c_str());

    //            _log_root.warn("TargetNVM::Get["+std::to_string(_soc_id)+"] for vertex id "+std::to_string(vertex_id)+
    //                           " offset "+std::to_string(offset) + " num_items "+std::to_string(num_items)+" mapped success");
                if(mapped.second == 0) {
                    _log_root.error("TargetNVM::Get["+std::to_string(_soc_id)+"] failed to find vertex "
                                    + std::to_string(vertex_id) + " in file id " + std::to_string(file_id));
                    throw std::out_of_range(
                            "failed to find vertex " + std::to_string(vertex_id) + " in file id " + std::to_string(file_id));
                }
                else if(mapped.second < offset+num_items) {
                    _log_root.error("TargetNVM::Get["+std::to_string(file_id)+"] invalid array offset "+
                                    std::to_string(offset+num_items)+
                                    " for vertex "+std::to_string(vertex_id)+
                                    " in soc "+std::to_string(file_id)+" that have "+
                                    std::to_string(mapped.second)+" items in nvm");
                    throw std::out_of_range("invalid array offset "+std::to_string(offset+num_items)+
                                            " for vertex "+std::to_string(vertex_id)+" in soc "+std::to_string(file_id)+" that have "+
                                            std::to_string(mapped.second)+" items in nvm");
                }
    //            _log_root.warn("TargetNVM::Get["+std::to_string(_soc_id)+"] for vertex id "+std::to_string(vertex_id)+
    //                           " offset "+std::to_string(offset) + " num_items "+std::to_string(num_items)+" array size = "+
    //                           std::to_string(mapped.second));
                memcpy(buffer.get(),mapped.first+offset,num_items*sizeof(T));
                _cache.insert(std::make_pair(vertex_name,std::make_pair(std::chrono::high_resolution_clock::now(),mapped.first)));
                _fifo.push_back(vertex_name);
                if(_fifo.size() > _fifo_size){
                    auto key = _fifo.front();
                    _fifo.pop_front();
                    _cache.erase(key);
                }
            }
            else{
            //    //_log_root.warn("TargetNVM"+std::to_string(_soc_id)+"::<"+typeid(T).name()+"> Get from cache");

                memcpy(buffer.get(),reinterpret_cast<T*>(cached->second.second)+offset,num_items*sizeof(T));
            }
            //_log_root.warn("TargetNVM"+std::to_string(_soc_id)+"::<"+typeid(T).name()+">Get End");
            return buffer;
        }
        catch(std::out_of_range e){
            _log_root.error("TargetNVM::Get["+std::to_string(_soc_id)+"] - failed to get buffer from file - "+std::to_string(file_id)+" "+std::string(e.what()));
            throw std::out_of_range("failed to find vertex "+std::to_string(vertex_id)+" with type "+ typeid(T).name()+" tag "+std::to_string(ClassTypeToUniqueChar<T>())+" in soc "+std::to_string(_soc_id));
        }
        catch(...)
        {
            _log_root.error("TargetNVM::Get["+std::to_string(_soc_id)+"] - failed to get buffer from file " +std::to_string(file_id)+" for vid "+std::to_string(vertex_id));
            throw;
        }

    }

    template<class T> std::unique_ptr<T[]> Get(tvg_vertex_id vertex_id, int offset, int num_items)
    {

        auto itr = _vertex_to_file_mapping.find(vertex_id);
        int file_id = -1;
        if(itr == _vertex_to_file_mapping.end()){
            _log_root.error("TargetNVM::Get["+std::to_string(_soc_id)+"] - failed to get buffer from NVM for vid "+std::to_string(vertex_id));
            throw std::out_of_range("TargetNVM::Get["+std::to_string(_soc_id)+"] - failed to get buffer from NVM for vid "+std::to_string(vertex_id));
        }
        else
            file_id = itr->second;

        return Get<T>(file_id,vertex_id,offset,num_items);
    }
//    template<class T> std::pair<tvg_long_counter,std::unique_ptr<T[]>> GetGenericVector(const std::string& vector_name)
//    {
//        //_log_root.error("TargetNVM::GetGenericVector["+std::to_string(_soc_id)+"] for key "+vector_name);
//        try
//        {
//            auto mapped = _nvm_region.find<T>(vector_name.c_str());
//            if(mapped.second == 0) {
//                _log_root.error("TargetNVM::GetGenericVector["+std::to_string(_soc_id)+"] failed to find key "+vector_name);
//                return std::make_pair(0, std::move(std::make_unique<T[]>(1)));
//            }
//            tvg_long_counter num_items = *(tvg_long_counter *)mapped.first;
//            auto buffer = std::make_unique<T[]>(num_items);
//
//            memcpy(buffer.get(),mapped.first+1,num_items*sizeof(T));
//            return std::make_pair(num_items,std::move(buffer));
//        }
//        catch(std::out_of_range e)
//        {
//            _log_root.error("TargetNVM::GetGenericVector ["+std::to_string(_soc_id)+"] - failed to get buffer - "+std::string(e.what()));
//            throw std::out_of_range("failed to find generic vector "+vector_name+" with type "+ typeid(T).name()+" in soc "+std::to_string(_soc_id));
//        }
//        catch(...)
//        {
//            _log_root.error("TargetNVM ["+std::to_string(_soc_id)+"] - failed to get buffer from NVM for handle "+vector_name);
//            throw;
//        }
//
//    }

    template<class T> std::pair<tvg_long_counter,std::unique_ptr<T[]>> GetGenericVector(int file_id,const std::string& vector_name)
    {
        //_log_root.debug("TargetNVM::GetGenericVector["+std::to_string(_soc_id)+"] for key "+vector_name);
        try
        {
            auto mapped = _nvm_region_map[file_id].find<T>(vector_name.c_str());
            if(mapped.second == 0) {
                _log_root.error("TargetNVM::GetGenericVector["+std::to_string(_soc_id)+"] failed to find key "+vector_name+" in file "+std::to_string(file_id));
                return std::make_pair(0, std::move(std::make_unique<T[]>(1)));
            }
            tvg_long_counter num_items = *(tvg_long_counter *)mapped.first;
            auto buffer = std::make_unique<T[]>(num_items);

            memcpy(buffer.get(),mapped.first+1,num_items*sizeof(T));
            return std::make_pair(num_items,std::move(buffer));
        }
        catch(std::out_of_range e)
        {
            _log_root.error("TargetNVM::GetGenericVector ["+std::to_string(file_id)+"] - failed to get buffer - "+std::string(e.what()));
            throw std::out_of_range("failed to find generic vector "+vector_name+" with type "+ typeid(T).name()+" in file id "+std::to_string(file_id));
        }
        catch(...)
        {
            _log_root.error("TargetNVM ["+std::to_string(file_id)+"] - failed to get buffer from NVM for handle "+vector_name);
            throw;
        }

    }
    template<class T> std::pair<tvg_long_counter,std::unique_ptr<T[]>> GetGenericVector(const std::string& vector_name)
    {
        //There is a problem here. I need to understand where to write meta data. what could happen here is that we have the same key in multiple files
        //if the nuber of reader is different from the number of writer this will cause an issue.
        return GetGenericVector<T>(_soc_id,vector_name);

    }

    void PrepareStorage()
    {
        // Do nothing here
    }



    template<class T> unsigned long NumCopiedItems(tvg_vertex_id vertex_id)
    {

        return _items_per_vertex[std::make_pair(vertex_id,ClassTypeToUniqueChar<T>())];
    }

    //store the size of the vector in cell # 0 - allocate N+1 size
    template<class T> tvg_long_counter SubmitGenericVector(const std::string & vector_name, std::unique_ptr<T[]> metadata_vector, tvg_long_counter num_items)
    {
        //DebugInfo::Instance()->PrintVector(metadata_vector, num_items);
        std::lock_guard<std::mutex> lock(_lock);
        _log_root.debug("TargetNVM::SubmitGenericVector["+std::to_string(_soc_id)+"] Start");
        T *mapped = nullptr;

        try
        {
            auto exist = _nvm_region_map[_soc_id].find<T>(vector_name.c_str());
            if(exist.second != 0)
            {
                _nvm_region_map[_soc_id].destroy_ptr(exist.first);

            }
            mapped = _nvm_region_map[_soc_id].construct<T>(vector_name.c_str())[num_items+1]();
        }
        catch (bip::bad_alloc)
        {
            if (_nvm_region_map[_soc_id].get_free_memory() < (num_items+1) * sizeof(T))
            {
                GrowRegion();
                mapped = _nvm_region_map[_soc_id].construct<T>(vector_name.c_str())[num_items+1]();
            }
            else
                throw;
        }

        *(tvg_long_counter*)(mapped) = num_items;
        memcpy(mapped+1, metadata_vector.get(), sizeof(T) * num_items);
        _nvm_region_map[_soc_id].flush();
        //_log_root.warn("TargetNVM::SubmitGenericVector["+std::to_string(_soc_id)+"] Finish");

        return num_items*sizeof(T);
    }

    template<class T> bool FixSubmitVertex(tvg_vertex_id vertex_id, int offset, std::unique_ptr<T> value)
    {
//        if(vertex_id == tvg_vertex_id(1000592))
//            _log_root.warn("TargetNVM::FixSubmitVertex["+std::to_string(_soc_id)+"] vertex "+std::to_string(vertex_id)+
//        " offset "+std::to_string(offset));
        DebugInfo::Instance()->PrintFixSubmit(vertex_id, offset, value);

        auto offset_array = std::vector<int>(1);
        offset_array[0] = offset;
        auto value_array = std::vector<std::unique_ptr<T>>(1);
        value_array[0] = std::move(value);
        return FixSubmitVertex(vertex_id,offset_array,value_array);


    }


    template<class T> bool FixSubmitVertex(tvg_vertex_id vertex_id, const std::vector<int> & offset_array, const std::vector<std::unique_ptr<T>> & value_array)
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto t1 = std::chrono::high_resolution_clock::now();
        auto T_type = ClassTypeToUniqueChar<T>();
        auto vertex_name = VertexName<T>(vertex_id);
        try
        {
            _vertex_to_file_mapping.insert(std::make_pair(vertex_id,_soc_id));
            auto itr = _items_per_vertex.find(std::make_pair(vertex_id,T_type));
            T * mapped_mem = nullptr;
            auto mapped = _nvm_region_map[_soc_id].find<T>(vertex_name.c_str());
            int num_allocated =0;
            //search for max offset
            auto max_offset = *std::max_element(offset_array.begin(), offset_array.end());

            if(mapped.second == 0){//new vertex
                //allocate until offset - all start will be uninit but that is fine
                mapped_mem =  _nvm_region_map[_soc_id].construct<T>(vertex_name.c_str())[max_offset+1]();
                _items_per_vertex[std::make_pair(vertex_id,T_type)] = max_offset+1;

            }
            else if((num_allocated = bip::managed_mapped_file::get_instance_length(mapped.first)) <= max_offset) {
                //check the amount of existing items - if there is a need - extend
                auto num_used = _items_per_vertex[std::make_pair(vertex_id,T_type)];
                mapped_mem = Reallocate<T>(vertex_id,(max_offset+1)-num_allocated,num_used);
                _items_per_vertex[std::make_pair(vertex_id,T_type)] = max_offset+1;
            }
            else {
                mapped_mem = mapped.first;
                _items_per_vertex[std::make_pair(vertex_id,T_type)] = std::max(_items_per_vertex[std::make_pair(vertex_id,T_type)],max_offset+1);
            }

            for(int i=0;i<offset_array.size();i++)
            {

                memcpy(mapped_mem + offset_array[i], value_array[i].get(), sizeof(T));
            }


            _nvm_region_map[_soc_id].flush();
//            if(vertex_id == tvg_vertex_id(1000072)){
//                _log_root.warn("TargetNVM::FixSubmitVertex["+std::to_string(_soc_id)+"] vertex name"+vertex_name+"of type "+
//                                       typeid(T).name()+" have "+
//                                       std::to_string(bip::managed_mapped_file::get_instance_length(mapped.first)) +
//                                       " items ?");
//            }


            return true;
        }
        catch(std::out_of_range e)
        {
            _log_root.error("TargetNVM::FixSubmitVertex ["+std::to_string(_soc_id)+"] - failed to fix vertex"+std::to_string(vertex_id)+" - "+std::string(e.what()));
            throw std::out_of_range("failed to find vertex "+std::to_string(vertex_id)+" tag "+std::to_string(ClassTypeToUniqueChar<T>())
                                    +" type "+typeid(T).name()+" in soc "+std::to_string(_soc_id));
        }
        catch(...)
        {
            auto mapped = _nvm_region_map[_soc_id].find<T>(vertex_name.c_str());
            auto used = _items_per_vertex[std::make_pair(vertex_id,T_type)];
            _log_root.error("TargetNVM::FixSubmitVertex ["+std::to_string(_soc_id)+"] - failed to fix vertex "+std::to_string(vertex_id)
                            +" amount used "+std::to_string(used)+" allocated "+std::to_string(bip::managed_mapped_file::get_instance_length(mapped.first)));
            throw;
        }

    }


    template<class T> tvg_long_counter Submit(tvg_vertex_id vertex_id, std::unique_ptr<T[]> vertex_buffer, tvg_long_counter num_items)
    {
//        if(vertex_id == tvg_vertex_id(1000592))
//            _log_root.warn("TargetNVM::Submit["+std::to_string(_soc_id)+"] vertex "+std::to_string(vertex_id)+
//                           " num_items "+std::to_string(num_items));
        //_log_root.error("TargetNVM::Submit ["+std::to_string(_soc_id)+"] -  vertex "+std::to_string(vertex_id)+" items "+std::to_string(num_items));

        DebugInfo::Instance()->Print(vertex_id, vertex_buffer, num_items);
        _vertex_to_file_mapping.insert(std::make_pair(vertex_id,_soc_id));

        std::lock_guard<std::mutex> lock(_lock);

        auto T_type = ClassTypeToUniqueChar<T>();


        if(_items_per_vertex.find(std::make_pair(vertex_id,T_type)) == _items_per_vertex.end()){
            _items_per_vertex[std::make_pair(vertex_id,T_type)] = 0;
        }


        std::string vertex_name = VertexName<T>(vertex_id);
//        _log_root.error("TargetNVM["+std::to_string(_soc_id)+"]::Submit -  vertex id "+std::to_string(vertex_id)+" tag "+std::to_string(T_type)+" type "+
//                                typeid(T).name()+" size "+std::to_string(vertex_name.size()));
        T* mapped = nullptr;
        try
        {
           mapped =  _nvm_region_map[_soc_id].find_or_construct<T>(vertex_name.c_str())[num_items]();
        }
        catch (bip::bad_alloc)
        {
            try
            {
                GrowRegion();
                mapped = _nvm_region_map[_soc_id].find_or_construct<T>(vertex_name.c_str())[num_items]();
            }
            catch (...)
            {
                _log_root.error("TargetNVM[" + std::to_string(_soc_id) + "]::Submit vid " + std::to_string(vertex_id) +
                                " free mem: " + std::to_string(_nvm_region_map[_soc_id].get_free_memory()) + " number of items " +
                                std::to_string(num_items) + "  sizeof(t): " + std::to_string(sizeof(T)));
                throw;
            }
        }

        tvg_long_counter allocated_amount = bip::managed_mapped_file::get_instance_length(mapped);
        tvg_long_counter used_amount = _items_per_vertex[std::make_pair(vertex_id,T_type)];

        if(num_items > allocated_amount - used_amount)
        {
            mapped = Reallocate<T>(vertex_id, num_items, used_amount);
        }

        memcpy(mapped+used_amount, vertex_buffer.get(), sizeof(T) * num_items);

        tvg_long_counter total_items = _items_per_vertex[std::make_pair(vertex_id,T_type)] + num_items;
        _items_per_vertex[std::make_pair(vertex_id,T_type)] = total_items;

        _nvm_region_map[_soc_id].flush();//make sure the memory was synced

        //_log_root.warn("TargetNVM::Submit["+std::to_string(_soc_id)+" Finish");
        return total_items * sizeof(T);//T::GetFromPair(_items_per_vertex[vertex_id]) * sizeof(T);
    }

private:

    short HashClassIdToChar(const std::string & class_id,int object_size)
    {
        char res = 0;
        for(auto &t : class_id){
            res ^= t;
        }

        //_log_root.warn("type "+class_id+" "+std::to_string(class_id.size()*256+res));
        _tagToSize[class_id.size()*256+res] = object_size;
        return class_id.size()*256+res;
    }
    template <class T> short ClassTypeToUniqueChar()
    {

        static short tag = HashClassIdToChar(typeid(T).name(),sizeof(T));
        return tag;
    }
    template <class T> std::string VertexName(tvg_vertex_id vertex_id)
    {
        return (std::to_string(vertex_id)+std::to_string(ClassTypeToUniqueChar<T>()));
    }
    std::string NvmRegionName() const
    {
        return _dir_path+"/nvm_region"+std::to_string(_soc_id)+".bin";

    }
    void GrowRegion(unsigned long long requested_amount=0)
    {
        //_log_root.warn("TargetNVM::GrowRegion by "+std::to_string(1<<_num_bit)+" bytes");
        //std::lock_guard<std::mutex> lock(_lock);//called within protected functions
        try {
            int num_grows = (requested_amount) /(1 << _num_bit) + 1;
            for(int i = 0 ; i < num_grows; i++) {//make multiple grows to fit at least the added item
                bool bret = bip::managed_mapped_file::grow(NvmRegionName().c_str(), 1 << _num_bit);
                if (!bret) {
                    _log_root.warn("TargetNVM::GrowRegion " + NvmRegionName() + " fail grow");
                    throw bip::bad_alloc();
                }
            }
            _nvm_region_map[_soc_id] = bip::managed_mapped_file(bip::open_only, NvmRegionName().c_str());
        }
        catch(...) {
            _log_root.warn("TargetNVM::GrowRegion "+NvmRegionName()+" fail grow or fail open");
            throw;
        }
    }
    template <class T> T* Reallocate(tvg_vertex_id vertex_id, unsigned long num_items,unsigned long number_used)
    {

        if(number_used > (1<<_num_bit)){
            _log_root.warn("TargetNVM["+std::to_string(_soc_id)+"]::Reallocate vid "+std::to_string(vertex_id)+" allocating a huge pile "+std::to_string(number_used));
        }
        try {

            std::string vertex_name = VertexName<T>(vertex_id);
            auto mapped = _nvm_region_map[_soc_id].find<T>(vertex_name.c_str());
            tvg_long_counter allocated_amount = bip::managed_mapped_file::get_instance_length(mapped.first);
            //int new_amount = 2 * (allocated_amount + num_items); //old version - cause a bit too much waste
            tvg_long_counter new_amount = 2 * allocated_amount + num_items;//slight improvement in waste not by much
//            int new_amount = 0;
//            if(2 * allocated_amount - number_used > num_items) {
//                new_amount = 2 * allocated_amount;
//                _log_root.info("TargetNVM["+std::to_string(_soc_id)+"]::Reallocate 2*allocate : allocated "+std::to_string(allocated_amount)+" used "+
//                                       std::to_string(number_used)+" needed "+std::to_string(num_items));
//            }
//            else {
//                new_amount = allocated_amount + 2*num_items;
//                _log_root.info("TargetNVM["+std::to_string(_soc_id)+"]::Reallocate allocate+2*needed : allocated "+std::to_string(allocated_amount)+" used "+
//                               std::to_string(number_used)+" needed "+std::to_string(num_items));
//            }

            //_log_root.warn("TargetNVM::Reallocate "+std::to_string(new_amount*sizeof(T))+" bytes for vertex "+std::to_string(vertex_id));
            //copy existing
            auto p = std::make_unique<T[]>(number_used);
            memcpy(p.get(), mapped.first, number_used * sizeof(T));
            if (_nvm_region_map[_soc_id].destroy<T>(vertex_name.c_str()) == false) {
                _log_root.error("TargetNVM["+std::to_string(_soc_id)+"]::Reallocate fail to destroy object " + vertex_name);
                throw std::logic_error("TargetNVM["+std::to_string(_soc_id)+"]::Reallocate fail to destroy object " + vertex_name);
            }
            T *new_p = nullptr;
            try {
                new_p = _nvm_region_map[_soc_id].construct<T>(vertex_name.c_str())[new_amount]();
            } catch (bip::bad_alloc) {
                auto stat = HeapUsage();
                _log_root.warn("TargetNVM::Reallocate - fail construct of size "+std::to_string(new_amount*sizeof(T))+" in SoC("+std::to_string(std::get<0>(stat))+") - heap_size:"+
                                std::to_string(std::get<1>(stat))+" allocated:"+std::to_string(std::get<2>(stat))+
                                " used:"+std::to_string(std::get<3>(stat)));
                GrowRegion(new_amount*sizeof(T));
                try {
                    new_p = _nvm_region_map[_soc_id].construct<T>(vertex_name.c_str())[new_amount]();
                }
                catch (...) {
                    _log_root.error("TargetNVM["+std::to_string(_soc_id)+"]::Reallocate exception after a grow trial of size "+std::to_string(new_amount*sizeof(T))+" vertex size ="+std::to_string(new_amount));
                    throw;
                }
            }

            memcpy(new_p,p.get(),number_used*sizeof(T));//copy old to new buffer and return the new pointer
            return new_p;
        }
        catch(...)
        {
            _log_root.error("TargetNVM::Reallocate generic throw ");
            throw;
        }
    }



};



#endif //TVG4TM_TARGETNVM_H
