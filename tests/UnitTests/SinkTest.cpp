#include <Common/DataTypes/DataTypeFactory.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include "SerializableOperator.pb.h"

using namespace std;

/**
 * @brief tests for sinks
 * @todo add tests for processing multiple buffers in a row
 */
namespace NES {

const size_t buffersManaged = 1024;
const size_t bufferSize = 4 * 1024;

class SinkTest : public testing::Test {
  public:
    size_t tupleCnt;
    SchemaPtr test_schema;
    size_t test_data_size;
    std::array<uint32_t, 8> test_data;
    bool write_result;
    std::string path_to_csv_file;
    std::string path_to_bin_file;

    static void SetUpTestCase() {
        NES::setupLogging("SinkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SinkTest class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down SinkTest class." << std::endl;
    }

    /* Called before a single test. */
    void SetUp() override {
        std::cout << "Setup SinkTest test case." << std::endl;
        test_schema = Schema::create()->addField("KEY", DataTypeFactory::createInt32())->addField("VALUE", DataTypeFactory::createUInt32());
        write_result = false;
        path_to_csv_file = "../tests/test_data/sink.csv";
        path_to_bin_file = "../tests/test_data/sink.bin";
    }

    /* Called after a single test. */
    void TearDown() override {
        std::cout << "Tear down SinkTest test case." << std::endl;
        std::remove(path_to_csv_file.c_str());
        std::remove(path_to_bin_file.c_str());
    }
};

TEST_F(SinkTest, testCSVSink) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr csvSink = createCSVFileSink(test_schema, path_to_csv_file, nodeEngine->getBufferManager(), true);
    for (size_t i = 0; i < 2; ++i) {
        for (size_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    cout << "bufferContent before write=" << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    write_result = csvSink->writeData(buffer);

    EXPECT_TRUE(write_result);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_csv_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_csv_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)),
                            (std::istreambuf_iterator<char>()));

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
}

TEST_F(SinkTest, testTextSink) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    TupleBuffer buffer = nodeEngine->getBufferManager()->getBufferBlocking();

    const DataSinkPtr binSink = createTextFileSink(test_schema, path_to_csv_file, nodeEngine->getBufferManager(), true);
    for (size_t i = 0; i < 5; ++i) {
        for (size_t j = 0; j < 5; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(25);
    write_result = binSink->writeData(buffer);
    EXPECT_TRUE(write_result);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    cout << "Buffer Content= " << bufferContent << endl;

    ifstream testFile(path_to_csv_file.c_str());
    EXPECT_TRUE(testFile.good());
    std::ifstream ifs(path_to_csv_file.c_str());
    std::string fileContent((std::istreambuf_iterator<char>(ifs)),
                            (std::istreambuf_iterator<char>()));

    cout << "File Content=" << fileContent << endl;
    EXPECT_EQ(bufferContent, fileContent);
    buffer.release();
}

TEST_F(SinkTest, testNESBinarySink) {
    NodeEnginePtr nodeEngine = std::make_shared<NodeEngine>();
    nodeEngine->start();

    auto buffer = nodeEngine->getBufferManager()->getBufferBlocking();
    const DataSinkPtr binSink = createBinaryNESFileSink(test_schema, path_to_bin_file, nodeEngine->getBufferManager(), true);
    for (size_t i = 0; i < 2; ++i) {
        for (size_t j = 0; j < 2; ++j) {
            buffer.getBuffer<uint64_t>()[j] = j;
        }
    }
    buffer.setNumberOfTuples(4);
    write_result = binSink->writeData(buffer);

    EXPECT_TRUE(write_result);
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema);
    cout << "Buffer Content= " << bufferContent << endl;

    //deserialize schema
    size_t idx = path_to_bin_file.rfind(".");
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
    if (ifs)
    {
        ifs.read(reinterpret_cast<char*>(deszBuffer.getBuffer()), deszBuffer.getBufferSize());
    }
    else{
        FAIL();
    }

    cout << "expected=" << endl << UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema) << endl;
    cout << "result=" << endl << UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema) << endl;

    cout << "File path = " << path_to_bin_file << " Content=" << UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema);
    EXPECT_EQ(UtilityFunctions::prettyPrintTupleBuffer(deszBuffer, test_schema), UtilityFunctions::prettyPrintTupleBuffer(buffer, test_schema));
}


}// namespace NES
