//
// Created by hayuney on 30/06/16.
//

#ifndef TVG_INDEX_SAVEITERATORS_H
#define TVG_INDEX_SAVEITERATORS_H


#include <Vertica.h>
//#include "tvg_index.h"
#include "General/Header/tvg_index.h"

using namespace Vertica;


class SaveIterators : public TransformFunction {


public:
    SaveIterators() : log_level(true) { }

private:

    bool log_level;

    void const log(ServerInterface &srvInterface, std::string msg) {
        log(srvInterface, msg.data());
    }

    void const log(ServerInterface &srvInterface, const char *msg) {
        if (log_level) {
            srvInterface.log("%s", msg);
        }
    }

    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter) {
        try {
            std::map<tvg_index,PartitionReader*> vertex_ptr_map;

            do {
//                pointer_array[index] =  &inputReader;
//                pointer_array[index]->copy(inputReader);

                auto source = inputReader.getIntRef(0);
//                auto count = inputReader.getIntRef(1);

                vertex_ptr_map[(tvg_index)source] = inputReader;
//                const VString *dest_array = inputReader.getStringPtr(2);
//                const VString *epoch_array = inputReader.getStringPtr(3);

            } while (inputReader.next());

//            for (int i = 0; i < array_size; ++i) {
//                log(srvInterface,"ptr: " + std::to_string(pointer_array[array_size]->getNumRows()));
//            }

            log(srvInterface,"map.size: " + std::to_string(vertex_ptr_map.size()));

            for (int i = 0; i < vertex_ptr_map.size(); ++i) {
                log(srvInterface,"map.size:1");
                auto ptr = vertex_ptr_map[i];
                log(srvInterface,"map.size:2");
                auto count = ptr->getIntRef(1);
                log(srvInterface,"map.size:3");
                log(srvInterface,"map.size:" + std::to_string(count));
//                log(srvInterface, "count ["+std::to_string(i)+"]: " + std::to_string(count));

            }




        } catch (std::exception &e) {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};


class SaveIteratorsFactory : public TransformFunctionFactory {
// Provide the function prototype information to the Vertica server (argument types + return types)
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addInt(); // source
        argTypes.addInt(); // array_size
        argTypes.addBinary(); //array destination
        argTypes.addBinary(); //array epoch

        returnType.addInt();
    }

// Provide return type length/scale/precision information (given the input
// type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvInterface, const SizedColumnTypes &inputTypes, SizedColumnTypes &outputTypes) {
//        outputTypes.addInt("source");
//        outputTypes.addInt("destination");
//        outputTypes.addInt("epoch");
        outputTypes.addInt("test");
    }

// Create an instance of the TransformFunction
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface) {
        return vt_createFuncObject<SaveIterators>(srvInterface.allocator);
    }
};


RegisterFactory(SaveIteratorsFactory);

#endif //TVG_INDEX_SAVEITERATORS_H
