//
// Created by hayuney on 26/06/16.
//

#ifndef TVG_INDEX_QUERYDS_H
#define TVG_INDEX_QUERYDS_H

#include <Vertica.h>
#include <boost/filesystem/path.hpp>
#include <log4cpp/Category.hh>
#include <log4cpp/PropertyConfigurator.hh>
#include <Graph/Header/MyGraphSelect.h>
#include "General/Header/tvg_index.h"
using namespace Vertica;


class QueryDS : public TransformFunction {

private:

    virtual void setup(ServerInterface &srvInterface,
                       const SizedColumnTypes &argTypes)
    {
        srvInterface.log("QueryDS::setup");
        std::string log_prop_file = "/tmp/log4cpp.properties";
        log4cpp::PropertyConfigurator::configure(log_prop_file);

    }


    // helpper function for splite string
    std::vector<std::string> &split(const std::string &s, char delim, std::vector<std::string> &elems) {
        std::stringstream ss(s);
        std::string item;
        while (getline(ss, item, delim)) {
            elems.push_back(item);
        }
        return elems;
    }

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter) {
        try {
            std::stringstream id;
            id << std::this_thread::get_id();

            log4cpp::Category::getRoot().debug("QueryDS:processPartition start srvInterface.getCurrentNodeName():" + srvInterface.getCurrentNodeName() + " thread:" + id.str());


            do {

                auto sources_array = inputReader.getStringPtr(0);
                auto start_time = inputReader.getIntRef(1);
                auto end_time = inputReader.getIntRef(2);
                auto deep = inputReader.getIntRef(3);

                std::vector<std::string> elements;
                auto sources = split(sources_array->str(),'|',elements);
                std::vector<VertexDepth> vNodes;
                for (int i =0 ;i<sources.size();i++){
                    VertexDepth verDeph(tvg_vertex_id(atoll(sources[i].c_str())));
                    vNodes.push_back(verDeph);
                }



                MyGraphSelect graphSelect;

                std::unique_ptr<std::list<MyEdgeAdvanced>> lResult = graphSelect.KHop(vNodes, tvg_time(start_time) ,tvg_time(end_time), deep);

                std::list<MyEdgeAdvanced>::iterator iterRes = lResult->begin();
                for ( ; iterRes != lResult->end() ; iterRes++)
                {
                    log4cpp::Category::getRoot().debug("LoadKHOP_Set: (" + iterRes->GetSource().GetString() + ", "+iterRes->GetDestination().GetString() + "), epoch=" +iterRes->GetEpoch().GetString() + ", depth=" + std::to_string(iterRes->GetDepth()));
                    outputWriter.setInt(0, iterRes->GetSource());
                    outputWriter.setInt(1, iterRes->GetDestination());
                    outputWriter.setInt(2, iterRes->GetEpoch());
                    outputWriter.next();

                }

            } while (inputReader.next());

            log4cpp::Category::getRoot().debug("QueryDS:processPartition finish srvInterface.getCurrentNodeName():" + srvInterface.getCurrentNodeName() + " thread:" + id.str());

        } catch (std::exception &e) {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};


class QueryDSFactory : public TransformFunctionFactory {
// Provide the function prototype information to the Vertica server (argument types + return types)
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addChar(); //list of start seed seperated via |
        argTypes.addInt(); // start time
        argTypes.addInt(); // end time
        argTypes.addInt(); // k deep

        returnType.addInt();  // source
        returnType.addInt();  // destination
        returnType.addInt();  // epoch
    }

// Provide return type length/scale/precision information (given the input
// type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvInterface, const SizedColumnTypes &inputTypes, SizedColumnTypes &outputTypes) {
        outputTypes.addInt("source");
        outputTypes.addInt("destination");
        outputTypes.addInt("epoch");
    }

// Create an instance of the TransformFunction
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) {
        return vt_createFuncObject<QueryDS>(srvInterface.allocator);
    }
};


RegisterFactory(QueryDSFactory);

#endif //TVG_INDEX_QUERYDS_H
