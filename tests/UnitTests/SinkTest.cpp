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
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <NodeEngine/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <ostream>

using namespace std;

/**
 * @brief tests for sinks
 * @todo add tests for processing multiple buffers in a row
 */
namespace NES {

class SinkTest : public testing::Test {
  public:
    SchemaPtr test_schema;
    std::array<uint32_t, 8> test_data;
    bool write_result;
    std::string path_to_csv_file;
    std::string path_to_bin_file;
    std::string path_to_osfile_file;

    static void SetUpTestCase() {
        NES::setupLogging("SinkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SinkTest class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down SinkTest class." << std::endl; }

    /* Called before a single test. */
    void SetUp() override {
        std::cout << "Setup SinkTest test case." << std::endl;
        test_schema =
            Schema::create()->addField("KEY", DataTypeFactory::createInt32())->addField("VALUE", DataTypeFactory::createUInt32());
        write_result = false;
        path_to_csv_file = "../tests/test_data/sink.csv";
        path_to_bin_file = "../tests/test_data/sink.bin";
        path_to_osfile_file = "../tests/test_data/testOs.txt";
    }

    /* Called after a single test. */
    void TearDown() override {
        std::cout << "Tear down SinkTest test case." << std::endl;
        std::remove(path_to_csv_file.c_str());
        std::remove(path_to_bin_file.c_str());
        std::remove(path_to_osfile_file.c_str());
    }
};

TEST_F(SinkTest, testCSVFileSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    WorkerContext wctx(NodeEngine::NesThread::getId());
    const DataSinkPtr csvSink = createCSVFileSink(test_schema, 0, nodeEngine, path_to_csv_file, true);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    cout << "bufferContent before write=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    write_result = csvSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_csv_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_csv_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    cout << "File Content=" << fileContent << endl;
    //search for each value
    string contentWOHeader = fileContent.erase(0, fileContent.find("\n") + 1);
    UtilityFunctions::findAndReplaceAll(contentWOHeader, "\n", ",");
    cout << "File Content shrinked=" << contentWOHeader << endl;

    stringstream ss(contentWOHeader);
    string item;
    while (getline(ss, item, ',')) {
        cout << item << endl;
        if (bufferContent.find(item) != std::string::npos) {
            cout << "found token=" << item << endl;
        } else {
            cout << "NOT found token=" << item << endl;
            EXPECT_TRUE(false);
        }
    }
    buffer.release();
}

TEST_F(SinkTest, testTextFileSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    WorkerContext wctx(NodeEngine::NesThread::getId());
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();

    const DataSinkPtr binSink = createTextFileSink(test_schema, 0, nodeEngine, path_to_csv_file, true);
    for (uint64_t i = 0; i < 5; ++i) {
        for (uint64_t j = 0; j < 5; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(25);
    write_result = binSink->writeData(buffer, wctx);
    EXPECT_TRUE(write_result);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_csv_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_csv_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    cout << "File Content=" << fileContent << endl;
    EXPECT_EQ(bufferContent, fileContent);
    buffer.release();
}

TEST_F(SinkTest, testNESBinaryFileSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    WorkerContext wctx(NodeEngine::NesThread::getId());
    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr binSink = createBinaryNESFileSink(test_schema, 0, nodeEngine, path_to_bin_file, true);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    write_result = binSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    cout << "Buffer Content= " << bufferContent << endl;

    //deserialize schema
    uint64_t idx = path_to_bin_file.rfind(".");
    std::string shrinkedPath = path_to_bin_file.substr(0, idx + 1);
    std::string schemaFile = shrinkedPath + "schema";
    cout << "load=" << schemaFile << endl;
    ifstream testFileSchema(schemaFile.c_str());
    EXPECT_TRUE(testFileSchema.good());
    SerializableSchema* serializedSchema = new SerializableSchema();
    serializedSchema->ParsePartialFromIstream(&testFileSchema);
    SchemaPtr ptr = SchemaSerializationUtil::deserializeSchema(serializedSchema);
    //test SCHEMA
    cout << "deserialized schema=" << ptr->toString() << endl;
    EXPECT_EQ(ptr->toString(), test_schema->toString());

    auto deszBuffer = nodeEngine->getBufferManager()->getBufferBlocking();
    deszBuffer.setNumberOfTuples(4);

    ifstream ifs(path_to_bin_file, ios_base::in | ios_base::binary);
    if (ifs) {
        ifs.read(reinterpret_cast<char*>(deszBuffer.getBuffer()), deszBuffer.getBufferSize());
    } else {
        FAIL();
    }

    cout << "expected=" << endl << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    cout << "result=" << endl << UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema) << endl;

    cout << "File path = " << path_to_bin_file
         << " Content=" << UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema);
    EXPECT_EQ(UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema),
              UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema));
    buffer.release();
}

TEST_F(SinkTest, testCSVPrintSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    std::filebuf fb;
    fb.open(path_to_osfile_file, std::ios::out);
    std::ostream os(&fb);
    WorkerContext wctx(NodeEngine::NesThread::getId());
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    auto csvSink = createCSVPrintSink(test_schema, 0, nodeEngine, os);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    cout << "bufferContent before write=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    write_result = csvSink->writeData(buffer, wctx);

    EXPECT_TRUE(write_result);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_osfile_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_osfile_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    cout << "File Content=" << fileContent << endl;
    //search for each value
    string contentWOHeader = fileContent.erase(0, fileContent.find("\n") + 1);
    UtilityFunctions::findAndReplaceAll(contentWOHeader, "\n", ",");
    cout << "File Content shrinked=" << contentWOHeader << endl;

    stringstream ss(contentWOHeader);
    string item;
    while (getline(ss, item, ',')) {
        cout << item << endl;
        if (bufferContent.find(item) != std::string::npos) {
            cout << "found token=" << item << endl;
        } else {
            cout << "NOT found token=" << item << endl;
            EXPECT_TRUE(false);
        }
    }
    fb.close();
    buffer.release();
}

TEST_F(SinkTest, testTextPrintSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    std::filebuf fb;
    fb.open(path_to_osfile_file, std::ios::out);
    std::ostream os(&fb);

    WorkerContext wctx(NodeEngine::NesThread::getId());
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();

    const DataSinkPtr binSink = createTextPrintSink(test_schema, 0, nodeEngine, os);
    for (uint64_t i = 0; i < 5; ++i) {
        for (uint64_t j = 0; j < 5; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(25);
    write_result = binSink->writeData(buffer, wctx);
    EXPECT_TRUE(write_result);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_osfile_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_osfile_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    cout << "File Content=" << fileContent << endl;
    EXPECT_EQ(bufferContent, fileContent.substr(0, fileContent.size() - 1));
    buffer.release();
    fb.close();
    buffer.release();
}

TEST_F(SinkTest, testCSVZMQSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    WorkerContext wctx(NodeEngine::NesThread::getId());
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr zmq_sink = createCSVZmqSink(test_schema, 0, nodeEngine, "localhost", 666555);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    cout << "buffer before send=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);

    // Create ZeroMQ Data Source.
    auto zmq_source =
        createZmqSource(test_schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), "localhost", 666555, 1);
    std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    bool receiving_finished = false;
    auto receiving_thread = std::thread([&]() {
        // Receive data.
        auto schemaData = zmq_source->receiveData();
        TupleBuffer bufSchema = schemaData.value();
        std::string schemaStr;
        schemaStr.assign(bufSchema.getBufferAs<char>(), bufSchema.getNumberOfTuples());
        cout << "Schema=" << schemaStr << endl;
        EXPECT_EQ(UtilityFunctions::toCSVString(test_schema), schemaStr);

        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();
        cout << "Buffer=" << bufData.getBufferAs<char>() << endl;

        std::string bufferContent = UtilityFunctions::printTupleBufferAsCSV(buffer, test_schema);
        std::string dataStr;
        dataStr.assign(bufData.getBufferAs<char>(), bufData.getNumberOfTuples());
        cout << "Buffer Content received= " << bufferContent << endl;
        EXPECT_EQ(bufferContent, dataStr);
        receiving_finished = true;
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
    buffer.release();
}

TEST_F(SinkTest, testTextZMQSink) {
    WorkerContext wctx(NodeEngine::NesThread::getId());
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr zmq_sink = createTextZmqSink(test_schema, 0, nodeEngine, "localhost", 666555);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    cout << "buffer before send=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);

    // Create ZeroMQ Data Source.
    auto zmq_source =
        createZmqSource(test_schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), "localhost", 666555, 1);
    std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    bool receiving_finished = false;
    auto receiving_thread = std::thread([&]() {
        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();

        std::string bufferContent = UtilityFunctions::printTupleBufferAsText(bufData);
        cout << "Buffer Content received= " << bufferContent << endl;
        EXPECT_EQ(bufferContent, UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema));
        receiving_finished = true;
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
    buffer.release();
}

TEST_F(SinkTest, testBinaryZMQSink) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    WorkerContext wctx(NodeEngine::NesThread::getId());
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr zmq_sink = createBinaryZmqSink(test_schema, 0, nodeEngine, "localhost", 666555, false);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    cout << "buffer before send=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);

    // Create ZeroMQ Data Source.
    auto zmq_source =
        createZmqSource(test_schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), "localhost", 666555, 1);
    std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    auto receiving_thread = std::thread([&]() {
        auto schemaData = zmq_source->receiveData();
        TupleBuffer bufSchema = schemaData.value();
        SerializableSchema* serializedSchema = new SerializableSchema();
        serializedSchema->ParseFromArray(bufSchema.getBuffer(), bufSchema.getNumberOfTuples());
        SchemaPtr ptr = SchemaSerializationUtil::deserializeSchema(serializedSchema);
        EXPECT_EQ(ptr->toString(), test_schema->toString());

        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();
        cout << "rec buffer tups=" << bufData.getNumberOfTuples()
             << " content=" << UtilityFunctions::prettyPrintTupleBuffer(bufData, test_schema) << endl;
        cout << "ref buffer tups=" << buffer.getNumberOfTuples()
             << " content=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema) << endl;
        EXPECT_EQ(UtilityFunctions::prettyPrintTupleBuffer(bufData, test_schema),
                  UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema));
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
    buffer.release();
}

TEST_F(SinkTest, testWatermarkForZMQ) {
    WorkerContext wctx(NodeEngine::NesThread::getId());
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    buffer.setWatermark(1234567);
    const DataSinkPtr zmq_sink = createBinaryZmqSink(test_schema, 0, nodeEngine, "localhost", 666555, false);
    for (uint64_t i = 1; i < 3; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j * i] = j;
        }
    }
    buffer.setNumberOfTuples(4);

    // Create ZeroMQ Data Source.
    auto zmq_source =
        createZmqSource(test_schema, nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), "localhost", 666555, 1);
    std::cout << zmq_source->toString() << std::endl;

    // Start thread for receivingh the data.
    auto receiving_thread = std::thread([&]() {
        auto schemaData = zmq_source->receiveData();

        auto bufferData = zmq_source->receiveData();
        TupleBuffer bufData = bufferData.value();
        EXPECT_EQ(bufData.getWatermark(), 1234567);
    });

    // Wait until receiving is complete.
    zmq_sink->writeData(buffer, wctx);
    receiving_thread.join();
    buffer.release();
}

TEST_F(SinkTest, testWatermarkCsvSource) {
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
    auto nodeEngine = NodeEngine::create("127.0.0.1", 31337, streamConf);
    WorkerContext wctx(NodeEngine::NesThread::getId());
    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    buffer.setWatermark(1234567);

    const DataSinkPtr csvSink = createCSVFileSink(test_schema, 0, nodeEngine, path_to_csv_file, true);
    for (uint64_t i = 0; i < 2; ++i) {
        for (uint64_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    cout << "watermark=" << buffer.getWatermark() << endl;
    write_result = csvSink->writeData(buffer, wctx);

    EXPECT_EQ(buffer.getWatermark(), 1234567);
    buffer.release();
}

}// namespace NES
