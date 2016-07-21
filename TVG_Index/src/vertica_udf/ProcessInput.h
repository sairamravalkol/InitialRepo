//
// Created by hayuney on 26/06/16.
//

#ifndef TVG_INDEX_PART_BINARY_H
#define TVG_INDEX_PART_BINARY_H

#include <thread>

#include <Vertica.h>
#include <Graph/Header/MyGraph.h>
#include <log4cpp/PropertyConfigurator.hh>
#include "General/Header/tvg_index.h"


using namespace Vertica;

class ProcessInput : public TransformFunction {
private:
    MyGraph _graph;

public:

    virtual void setup(ServerInterface &srvInterface,
                       const SizedColumnTypes &argTypes) {
        std::string log_prop_file = "/tmp/log4cpp.properties";
        log4cpp::PropertyConfigurator::configure(log_prop_file);
    }

    virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
        _graph.Finalize();
    }


    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter) {
        std::stringstream id;
        id << std::this_thread::get_id();
        log4cpp::Category::getRoot().debug("ProcessInput::processPartition start thread id: (" + id.str() + ") processing for source: " + std::to_string(inputReader.getIntRef(0)));
        try {

            int index = 0;
            vint source = 0;
            vint destination = 0;
            vint epoch = 0;


            std::unique_ptr<Modification> event;
            do {
                source = inputReader.getIntRef(0);
                destination = inputReader.getIntRef(1);
                epoch = inputReader.getIntRef(2);

                // creating modification with random UID
                event = std::make_unique<Modification>(Modification(random()));
                event->SetSource(source);
                event->SetDestination(destination);
                event->SetEpoc(epoch);

                _graph.AddDirectedModification(std::move(event));
                index++;

            } while (inputReader.next());
            log4cpp::Category::getRoot().debug("_graph.GetVertexCount(): %llu" + _graph.GetVertexCount());

            outputWriter.setInt(0, index);
            outputWriter.next();

        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
        log4cpp::Category::getRoot().debug("ProcessInput::processPartition finish tid: (" + id.str() + ") processing for source: " + std::to_string(inputReader.getIntRef(0)));
    }
};

class ProcessInputFactory : public TransformFunctionFactory {
// Provide the function prototype information to the Vertica server (argument types + return types)
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addInt();  // source
        argTypes.addInt();  // destination
        argTypes.addInt();  // epoch

        returnType.addInt();    // partition length
    }

// Provide return type length/scale/precision information (given the input
// type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes) {
        outputTypes.addInt("partition_length");
    }

// Create an instance of the TransformFunction
    virtual TransformFunction *createTransformFunction(
            ServerInterface &srvInterface) { return vt_createFuncObject<ProcessInput>(srvInterface.allocator); }

};

RegisterFactory(ProcessInputFactory);

#endif //TVG_INDEX_PART_BINARY_H
