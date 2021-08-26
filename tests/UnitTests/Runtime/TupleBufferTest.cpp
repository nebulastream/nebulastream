/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Schema.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Declarations/Declaration.hpp>
#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/Statement.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/GeneratorSource.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;
namespace NES {

class TupleBufferTest : public testing::Test {
  public:
    Runtime::BufferManagerPtr bufferManager;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("TupleBufferTest.log", NES::LOG_DEBUG);
        NES_INFO("SetUpTestCase TupleBufferTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1024); }

    /* Will be called after a test is executed. */
    void TearDown() override { bufferManager->destroy(); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("TearDownTestCase TupleBufferTest test case."); }
};

TEST_F(TupleBufferTest, testPrintingOfTupleBuffer) {

    struct __attribute__((packed)) MyTuple {
        uint64_t i64;
        float f;
        double d;
        uint32_t i32;
        char s[5];
    };

    auto optBuf = bufferManager->getBufferNoBlocking();
    auto buf = *optBuf;
    //    MyTuple* my_array = (MyTuple*)malloc(5 * sizeof(MyTuple));
    auto* my_array = buf.getBuffer<MyTuple>();
    for (unsigned int i = 0; i < 5; ++i) {
        my_array[i] = MyTuple{i, float(0.5F * i), double(i * 0.2), i * 2, "1234"};
        std::cout << my_array[i].i64 << "|" << my_array[i].f << "|" << my_array[i].d << "|" << my_array[i].i32 << "|"
                  << std::string(my_array[i].s, 5) << std::endl;
    }
    buf.setNumberOfTuples(5);

    SchemaPtr s = Schema::create()
                      ->addField("i64", DataTypeFactory::createUInt64())
                      ->addField("f", DataTypeFactory::createFloat())
                      ->addField("d", DataTypeFactory::createDouble())
                      ->addField("i32", DataTypeFactory::createUInt32())
                      ->addField("s", DataTypeFactory::createFixedChar(5));

    // XXX
    std::string reference = "+----------------------------------------------------+\n"
                            "|i64:UINT64|f:FLOAT32|d:FLOAT64|i32:UINT32|s:CHAR[5]|\n"
                            "+----------------------------------------------------+\n"
                            "|0|0.000000|0.000000|0|1234 |\n"
                            "|1|0.500000|0.200000|2|1234 |\n"
                            "|2|1.000000|0.400000|4|1234 |\n"
                            "|3|1.500000|0.600000|6|1234 |\n"
                            "|4|2.000000|0.800000|8|1234 |\n"
                            "+----------------------------------------------------+";

    std::string result = UtilityFunctions::prettyPrintTupleBuffer(buf, s);
    std::cout << "RES=" << result << std::endl;
    NES_DEBUG("Reference size=" << reference.size() << " content=\n" << reference);
    NES_DEBUG("Result size=" << result.size() << " content=\n" << result);
    NES_DEBUG("----");
    EXPECT_EQ(reference.size(), result.size());
    //    EXPECT_EQ(reference, result);//TODO fix bug
}

TEST_F(TupleBufferTest, testCopyAndSwap) {
    auto buffer = bufferManager->getBufferBlocking();
    for (auto i = 0; i < 10; ++i) {
        *buffer.getBuffer<uint64_t>() = 0;
        buffer = bufferManager->getBufferBlocking();
    }
}

}// namespace NES
