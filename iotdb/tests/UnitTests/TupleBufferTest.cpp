#include <cassert>
#include <iostream>
#include <math.h>
#include "gtest/gtest.h"
#include <Services/CoordinatorService.hpp>
#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Types/DataTypes.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/PipelineContext.hpp>
#include <QueryCompiler/PipelineStage.hpp>
#include <QueryCompiler/CCodeGenerator/BinaryOperatorStatement.hpp>
#include <QueryCompiler/CCodeGenerator/Declaration.hpp>
#include <QueryCompiler/CCodeGenerator/FileBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/FunctionBuilder.hpp>
#include <QueryCompiler/CCodeGenerator/Statement.hpp>
#include <QueryCompiler/CCodeGenerator/UnaryOperatorStatement.hpp>
#include <QueryCompiler/Compiler/CompiledExecutablePipeline.hpp>
#include <QueryCompiler/Compiler/SystemCompilerCompiledCode.hpp>
#include <Windows/WindowHandler.hpp>
#include <SourceSink/SinkCreator.hpp>
#include <SourceSink/GeneratorSource.hpp>
#include <SourceSink/DefaultSource.hpp>
#include <NodeEngine/MemoryLayout/MemoryLayout.hpp>
#include <NodeEngine/BufferManager.hpp>

namespace NES {

class TupleBufferTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup TupleBufferTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        setupLogging();
        std::cout << "Setup TupleBufferTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down TupleBufferTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down TupleBufferTest test class." << std::endl;
    }
  protected:
    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
            new log4cxx::PatternLayout(
                "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "TupleBufferTest.log");
        log4cxx::FileAppenderPtr file(
            new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(
            new log4cxx::ConsoleAppender(layoutPtr));

        // set log level
        NES::NESLogger->setLevel(log4cxx::Level::getDebug());
        //    logger->setLevel(log4cxx::Level::getInfo());

        // add appenders and other will inherit the settings
        NES::NESLogger->addAppender(file);
        NES::NESLogger->addAppender(console);
    }
};

TEST_F(TupleBufferTest, testPrintingOfTupleBuffer) {

    struct __attribute__((packed)) MyTuple {
        uint64_t i64;
        float f;
        double d;
        uint32_t i32;
        char s[12];
    };

    MyTuple* my_array = (MyTuple*) malloc(5*sizeof(MyTuple));
    for (unsigned int i = 0; i < 5; ++i) {
        my_array[i] = MyTuple{i, float(0.5f*i), double(i*0.2), i*2, "1234"};
        std::cout << my_array[i].i64 << "|" << my_array[i].f << "|" << my_array[i].d << "|" << my_array[i].i32 << "|"
                  << std::string(my_array[i].s, 12) << std::endl;
    }

    TupleBuffer buf{my_array, 5*sizeof(MyTuple), sizeof(MyTuple), 5};
    Schema s = Schema::create()
        .addField("i64", UINT64)
        .addField("f", FLOAT32)
        .addField("d", FLOAT64)
        .addField("i32", UINT32)
        .addField("s", 12);

    std::string reference = "+----------------------------------------------------+\n"
                            "|i64:UINT64|f:FLOAT32|d:FLOAT64|i32:UINT32|s:CHAR|\n"
                            "+----------------------------------------------------+\n"
                            "|0|0.000000|0.000000|0|1234|\n"
                            "|1|0.500000|0.200000|2|1234|\n"
                            "|2|1.000000|0.400000|4|1234|\n"
                            "|3|1.500000|0.600000|6|1234|\n"
                            "|4|2.000000|0.800000|8|1234|\n"
                            "+----------------------------------------------------+";

    std::string result = NES::toString(buf, s);
    std::cout << "'" << reference << "'" << reference.size() << std::endl;
    std::cout << "'" << result << "'" << result.size() << std::endl;

    EXPECT_EQ(reference.size(), result.size());
    EXPECT_EQ(reference, result);

    free(my_array);
}

}


