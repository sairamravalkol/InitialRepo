//
// Created by hayuney on 26/06/16.
//

#ifndef TVG_INDEX_PART_BINARY_H
#define TVG_INDEX_PART_BINARY_H

#include <thread>

#include <Vertica.h>
#include "tvg_index.h"

#define BINARY_MAX 65000

using namespace Vertica;

class ProcessInput : public TransformFunction {

public:

    ProcessInput() : log_level(true) { }

    bool log_level;

    void const log(ServerInterface &srvInterface, const std::string msg) {
        log(srvInterface, msg.data());
    }

    void const log(ServerInterface &srvInterface, const char *msg)  {
        if (log_level) {
            srvInterface.log("%s", msg);
        }
    }


    virtual void processPartition(ServerInterface &srvInterface,
                                  PartitionReader &inputReader,
                                  PartitionWriter &outputWriter) {


        std::stringstream id;
        id << std::this_thread::get_id();
        std::string msg = "Part::processPartition start tid: (" + id.str() + ") processing for source: " + std::to_string(inputReader.getIntRef(0));
        log(srvInterface, msg);

        try {
            tvg_index *destination_array;
            tvg_index *epoch_array;

            destination_array = new tvg_index[BINARY_MAX / sizeof(tvg_index)];
            epoch_array = new tvg_index[BINARY_MAX / sizeof(tvg_index)];

            int index = 0;
            vint source = 0;
            vint destination = 0;
            vint epoch = 0;
            do {
                source = inputReader.getIntRef(0);
                destination = inputReader.getIntRef(1);
                epoch = inputReader.getIntRef(2);

                destination_array[index] = destination;
                epoch_array[index] = epoch;
                index++;

            } while (inputReader.next());


            log(srvInterface, "count of dest : " + std::to_string(index));
            for (int i = 0; i < index; ++i) {
                log(srvInterface, "dest: " + std::to_string(destination_array[i].Get()));
            }

            outputWriter.setInt(0, source);
            outputWriter.setInt(1, index);
            outputWriter.getStringRef(2).copy((char *) destination_array, index * sizeof(tvg_index));
            outputWriter.getStringRef(3).copy((char *) epoch_array, index * sizeof(tvg_index));
            outputWriter.next();

        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
        msg = "Part::processPartition finish tid: (" + id.str() + ") processing for source: " + std::to_string(inputReader.getIntRef(0));
        log(srvInterface, msg);
    }
};

class ProcessInputFactory : public TransformFunctionFactory {
// Provide the function prototype information to the Vertica server (argument types + return types)
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addInt();
        argTypes.addInt();
        argTypes.addInt();

        returnType.addInt();
        returnType.addInt();
        returnType.addBinary(); // dest
        returnType.addBinary(); // epochs
    }

// Provide return type length/scale/precision information (given the input
// type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes) {
        outputTypes.addInt("source");
        outputTypes.addInt("array_count");
        outputTypes.addBinary(BINARY_MAX, "destination_array");
        outputTypes.addBinary(BINARY_MAX, "epoch_array");
    }

// Create an instance of the TransformFunction
    virtual TransformFunction *createTransformFunction(
            ServerInterface &srvInterface) { return vt_createFuncObject<ProcessInput>(srvInterface.allocator); }

};

RegisterFactory(ProcessInputFactory);

#endif //TVG_INDEX_PART_BINARY_H
