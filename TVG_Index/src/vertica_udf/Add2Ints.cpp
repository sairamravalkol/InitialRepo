//
// Created by hayune on 6/20/16.
//


#include <Vertica.h>

class Add2Ints : public Vertica::ScalarFunction
{
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
                vt_report_error(0, "Function only accept 2 arguments, but %zu provided", argReader.getNumCols());

            // While we have inputs to process
            do {
                const Vertica::vint a = argReader.getIntRef(0);
                const Vertica::vint b = argReader.getIntRef(1);
                resWriter.setInt(a + b);
                resWriter.next();
            } while (argReader.next());
        } catch (std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class Add2Numbers : public Vertica::ScalarFunctionFactory
{
    // return an instance of Add2Ints to perform the actual addition.
    virtual Vertica::ScalarFunction *createScalarFunction(Vertica::ServerInterface &interface)
    {
        return Vertica::vt_createFuncObject<Add2Ints>(interface.allocator);
    }

    virtual void getPrototype(Vertica::ServerInterface &interface,
                              Vertica::ColumnTypes &argTypes,
                              Vertica::ColumnTypes &returnType)
    {
        argTypes.addInt();
        argTypes.addInt();
        returnType.addInt();
    }
};

RegisterFactory(Add2Numbers);

