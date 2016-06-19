//
// Created by hayune on 6/19/16.
//


#include "Vertica.h"

class  helloworld : public Vertica::ScalarFunction {

        public:

        /*
         * This method processes a block of rows in a single invocation.
         *
         * The inputs are retrieved via argReader
         * The outputs are returned via resWriter
         */
        virtual void processBlock(Vertica::ServerInterface &srvInterface,
                                  Vertica::BlockReader &argReader,
                                  Vertica::BlockWriter &resWriter)
        {
            try {
                // Basic error checking
                if (argReader.getNumCols() != 2)
                    vt_report_error(0, "Function only accept 2 arguments, but %zu provided",
                                    argReader.getNumCols());

                // While we have inputs to process
                do {
                    const Vertica::vint a = argReader.getIntRef(0);
                    const Vertica::vint b = argReader.getIntRef(1);
                    resWriter.setInt(a+b);
                    resWriter.next();
                } while (argReader.next());
            } catch(std::exception& e) {
                // Standard exception. Quit.
                vt_report_error(0, "Exception while processing block: [%s]", e.what());
            }
        }
    };
