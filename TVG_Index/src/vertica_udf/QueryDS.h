//
// Created by hayuney on 26/06/16.
//

#ifndef TVG_INDEX_QUERYDS_H
#define TVG_INDEX_QUERYDS_H

#include <Vertica.h>
#include "tvg_index.h"

using namespace Vertica;


#define BINARY_MAX 65000


class QueryDS : public TransformFunction {

public:
    QueryDS() : log_level(true) { }

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
            log(srvInterface, "test me!!!!");
            do {

                auto source = inputReader.getIntRef(0);
                auto count = inputReader.getIntRef(1);
                const VString *dest_array = inputReader.getStringPtr(2);
                const VString *epoch_array = inputReader.getStringPtr(3);


                log(srvInterface, std::string("source: %u", source));
                log(srvInterface, std::string("count: %u", count));
                log(srvInterface, std::string(""));

                tvg_index *destinations;
                destinations = (tvg_index *) dest_array->data();

                tvg_index *epochs;
                epochs = (tvg_index *) epoch_array->data();
                for (int i = 0; i < count; ++i) {
                    outputWriter.setInt(0, source);
                    outputWriter.setInt(1, destinations[i].get_index());
                    outputWriter.setInt(2, epochs[i].get_index());
                    outputWriter.next();
                }

            } while (inputReader.next());

        } catch (std::exception &e) {
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};


class QueryDSFactory : public TransformFunctionFactory {
// Provide the function prototype information to the Vertica server (argument types + return types)
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addInt(); // source
        argTypes.addInt(); // array_size
        argTypes.addBinary(); //array destination
        argTypes.addBinary(); //array epoch

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
