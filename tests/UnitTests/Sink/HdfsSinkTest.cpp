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

#include "SerializableOperator.pb.h"
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <HDFS/hdfs.h>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <boost/algorithm/string.hpp>
#include <gtest/gtest.h>
#include <ostream>
#include <limits.h> /* PATH_MAX */

using namespace std;

/**
 * @brief tests for hdfs sink
 */
namespace NES {
using NodeEngine::TupleBuffer;

class HdfsSinkTest : public testing::Test {
  public:
    SchemaPtr test_schema;
    std::array<uint32_t, 8> test_data;
    bool write_result;
    char *path_to_bin_file;
    char *path_to_csv_file;
    std::ifstream input;

    static void SetUpTestCase() {
        NES::setupLogging("HdfsSinkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup HdfsSinkTest class.");
    }

    static void TearDownTestCase() { NES_DEBUG("Tear down HdfsSinkTest class"); }

    void SetUp() override {
        NES_DEBUG("Setup HdfsSinkTest test case");
        test_schema =
            Schema::create()->addField("KEY", DataTypeFactory::createInt32())->addField("VALUE", DataTypeFactory::createUInt32());
        write_result = false;
        path_to_bin_file = "/testData/sink.bin";
        path_to_csv_file = "/testData/sink.csv";
    }

    /* Called after a single test. */
    void TearDown() override {
        NES_DEBUG("Tear down HdfsSinkTest test case");
        hdfsBuilder *bld = hdfsNewBuilder();
        hdfsBuilderSetForceNewInstance(bld);
        hdfsBuilderSetNameNode(bld, "192.168.1.104");
        hdfsBuilderSetNameNodePort(bld, 9000);
        hdfsBuilderSetUserName(bld, "hdoop");
        hdfsFS fs = hdfsBuilderConnect(bld);
        hdfsDelete(fs, path_to_bin_file, 0);
        hdfsDelete(fs, path_to_csv_file, 0);
    }

};

TEST_F(HdfsSinkTest, testHdfsBinSink) {
    // Set up hdfs builder
    hdfsBuilder *bld = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetNameNode(bld, "192.168.1.104");
    hdfsBuilderSetNameNodePort(bld, 9000);
    hdfsBuilderSetUserName(bld, "hdoop");

    // Create hdfs test file
    hdfsFS fs_aux = hdfsBuilderConnect(bld);
    hdfsFile outputFile = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs_aux, path_to_bin_file, O_WRONLY | O_CREAT));
    hdfsWrite(fs_aux, outputFile, "", 0);
    hdfsFlush(fs_aux, outputFile);
    hdfsCloseFile(fs_aux, outputFile);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr hdfsSink = createHdfsBinSink(test_schema, 0, nodeEngine, path_to_bin_file, true);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    NES_DEBUG("HdfsSinkTest buffer content: " << bufferContent);
    write_result = hdfsSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);

    //deserialize schema
    std::string path = path_to_bin_file;
    uint64_t idx = path.rfind(".");
    std::string shrinkedPath = path.substr(0, idx + 1);
    std::string schemaFile = "../tests/test_data/sink.schema";
    NES_DEBUG("HdfsSinkTest: load=" << schemaFile);
    ifstream testFileSchema(schemaFile.c_str());
    EXPECT_TRUE(testFileSchema.good());
    SerializableSchema* serializedSchema = new SerializableSchema();
    serializedSchema->ParsePartialFromIstream(&testFileSchema);
    SchemaPtr ptr = SchemaSerializationUtil::deserializeSchema(serializedSchema);
    //test SCHEMA
    NES_DEBUG("HdfsSinkTest: deserialized schema=" << ptr->toString());
    EXPECT_EQ(ptr->toString(), test_schema->toString());

    // Create new builder for the reader
    hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(builder);
    hdfsBuilderSetNameNode(builder, "192.168.1.104");
    hdfsBuilderSetNameNodePort(builder, 9000);
    hdfsBuilderSetUserName(builder, "hdoop");

    hdfsFS fs = hdfsBuilderConnect(builder);

    auto deszBuffer = nodeEngine->getBufferManager()->getBufferBlocking();
    deszBuffer.setNumberOfTuples(4);

    hdfsFile file = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, path_to_bin_file, O_RDONLY));

    if (file) {
        hdfsRead(fs, file, deszBuffer.getBuffer(), buffer.getBufferSize());
        std::string bufferContent2 = UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema);
        NES_DEBUG("HdfsSinkTest deszBuffer content: " << bufferContent2);
        hdfsCloseFile(fs, file);
    } else {
        FAIL();
    }

    NES_DEBUG("HdfsSinkTest: EXPECTED: " << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema));
    NES_DEBUG("HdfsSinkTest: RESULT: " << UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema));

    EXPECT_EQ(UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema),
              UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema));

    buffer.release();
}

TEST_F(HdfsSinkTest, testHdfsCSVSink) {
    // Set up hdfs builder
    hdfsBuilder *bld = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(bld);
    hdfsBuilderSetNameNode(bld, "192.168.1.104");
    hdfsBuilderSetNameNodePort(bld, 9000);
    hdfsBuilderSetUserName(bld, "hdoop");

    // Create hdfs test file
    hdfsFS fs_aux = hdfsBuilderConnect(bld);
    hdfsFile outputFile = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs_aux, path_to_csv_file, O_WRONLY | O_CREAT));
    hdfsWrite(fs_aux, outputFile, "", 0);
    hdfsFlush(fs_aux, outputFile);
    hdfsCloseFile(fs_aux, outputFile);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    NodeEngine::WorkerContext wctx(NodeEngine::NesThread::getId());
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr hdfsSink = createHdfsCSVSink(test_schema, 0, nodeEngine, path_to_csv_file, true);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    NES_DEBUG("HdfsSinkTest buffer content: " << bufferContent);
    write_result = hdfsSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);

    //deserialize schema
    std::string path = path_to_csv_file;
    uint64_t idx = path.rfind(".");
    std::string shrinkedPath = path.substr(0, idx + 1);
    std::string schemaFile = "../tests/test_data/sink.schema";
    NES_DEBUG("HdfsSinkTest: load=" << schemaFile);
    ifstream testFileSchema(schemaFile.c_str());
    EXPECT_TRUE(testFileSchema.good());
    SerializableSchema* serializedSchema = new SerializableSchema();
    serializedSchema->ParsePartialFromIstream(&testFileSchema);
    SchemaPtr ptr = SchemaSerializationUtil::deserializeSchema(serializedSchema);
    //test SCHEMA
    NES_DEBUG("HdfsSinkTest: deserialized schema=" << ptr->toString());
    EXPECT_EQ(ptr->toString(), test_schema->toString());

    // Create new builder for the reader
    hdfsBuilder *builder = hdfsNewBuilder();
    hdfsBuilderSetForceNewInstance(builder);
    hdfsBuilderSetNameNode(builder, "192.168.1.104");
    hdfsBuilderSetNameNodePort(builder, 9000);
    hdfsBuilderSetUserName(builder, "hdoop");

    hdfsFS fs = hdfsBuilderConnect(builder);

    auto deszBuffer = nodeEngine->getBufferManager()->getBufferBlocking();
    deszBuffer.setNumberOfTuples(4);

    hdfsFile file = hdfsStreamBuilderBuild(hdfsStreamBuilderAlloc(fs, path_to_csv_file, O_RDONLY));

    if (file) {
        hdfsRead(fs, file, deszBuffer.getBuffer(), buffer.getBufferSize());
        std::string bufferContent2 = UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema);
        NES_DEBUG("HdfsSinkTest deszBuffer content: " << bufferContent2);
        hdfsCloseFile(fs, file);
    } else {
        FAIL();
    }

    NES_DEBUG("HdfsSinkTest: EXPECTED: " << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema));
    NES_DEBUG("HdfsSinkTest: RESULT: " << UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema));

    EXPECT_EQ(UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema),
              UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema));

    buffer.release();
}

}// namespace NES
