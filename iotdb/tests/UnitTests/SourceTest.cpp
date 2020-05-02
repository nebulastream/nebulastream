#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <cstring>
#include <NodeEngine/QueryManager.hpp>

#include <Util/Logger.hpp>
#include <API/Types/DataTypes.hpp>
#include <SourceSink/SourceCreator.hpp>

namespace NES {

struct __attribute__((packed)) ysbRecord {
    char user_id[16];
    char page_id[16];
    char campaign_id[16];
    char ad_type[9];
    char event_type[9];
    int64_t current_ms;
    uint32_t ip;

    ysbRecord() {
        event_type[0] = '-';  // invalid record
        current_ms = 0;
        ip = 0;
    }

    ysbRecord(const ysbRecord& rhs) {
        memcpy(&user_id, &rhs.user_id, 16);
        memcpy(&page_id, &rhs.page_id, 16);
        memcpy(&campaign_id, &rhs.campaign_id, 16);
        memcpy(&ad_type, &rhs.ad_type, 9);
        memcpy(&event_type, &rhs.event_type, 9);
        current_ms = rhs.current_ms;
        ip = rhs.ip;
    }
};
// size 78 bytes

typedef const DataSourcePtr (* createFileSourceFuncPtr)(SchemaPtr,
                                                        BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                                        const std::string&);

typedef const DataSourcePtr (* createSenseSourceFuncPtr)(SchemaPtr,
                                                         BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                                         const std::string&);

typedef const DataSourcePtr (* createCSVSourceFuncPtr)(const SchemaPtr,
                                                       BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                                                       const std::string&,
                                                       const std::string&,
                                                       size_t, size_t);

class SourceTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("SourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SourceTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down SourceTest test class." << std::endl;
    }

    void TearDown() {
    }
};

TEST_F(SourceTest, testBinarySource) {
    QueryManagerPtr queryManager = std::make_shared<QueryManager>();
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(4096, 1024);


    std::string path_to_file =
        "../tests/test_data/ysb-tuples-100-campaign-100.bin";
    createFileSourceFuncPtr funcPtr = &createBinaryFileSource;

    SchemaPtr schema =
        Schema::create()->addField("user_id", 16)->addField("page_id", 16)->addField(
                "campaign_id", 16)->addField("ad_type", 9)->addField("event_type", 9)
            ->addField("current_ms", UINT64)->addField("ip", INT32);

    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = bufferManager->getBufferSize();
    size_t num_of_buffers = 1;
    uint64_t num_tuples_to_process = num_of_buffers * (buffer_size/tuple_size);

    assert(buffer_size > 0);

    const DataSourcePtr source = (*funcPtr)(schema, bufferManager, queryManager, path_to_file);

    while (source->getNumberOfGeneratedBuffers() < num_of_buffers) {
        auto optBuf = source->receiveData();
        size_t i = 0;
        while (i*tuple_size < buffer_size - tuple_size && !!optBuf ) {
            ysbRecord record(
                *((ysbRecord*) (optBuf->getBufferAs<char>() + i*tuple_size)));
            std::cout << "i=" << i << " record.ad_type: " << record.ad_type << ", record.event_type: " << record.event_type << std::endl;
            EXPECT_STREQ(record.ad_type, "banner78");
            EXPECT_TRUE(
                (!strcmp(record.event_type, "view")
                    || !strcmp(record.event_type, "click")
                    || !strcmp(record.event_type, "purchase")));
            i++;
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedTuples(), num_tuples_to_process);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), num_of_buffers);
    std::cout << "Success" << std::endl;
}

TEST_F(SourceTest, testCSVSource) {
    QueryManagerPtr queryManager = std::make_shared<QueryManager>();
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(4096, 1024);

    std::string path_to_file =
        "../tests/test_data/ysb-tuples-100-campaign-100.csv";
    createCSVSourceFuncPtr funcPtr = &createCSVFileSource;

    const std::string& del = ",";
    size_t num = 1;
    size_t frequency = 1;
    SchemaPtr schema =
        Schema::create()->addField("user_id", 16)->addField("page_id", 16)->addField(
                "campaign_id", 16)->addField("ad_type", 9)->addField("event_type", 9)
            ->addField("current_ms", UINT64)->addField("ip", INT32);


    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = bufferManager->getBufferSize();
    size_t num_of_buffers = 10;
    uint64_t num_tuples_to_process = num_of_buffers * (buffer_size/tuple_size);

    //    uint64_t buffer_size = num_tuples_to_process*tuple_size / num_of_buffers;
    assert(buffer_size > 0);

    const DataSourcePtr source = (*funcPtr)(schema, bufferManager, queryManager, path_to_file, del, num,
                                            frequency);

    while (source->getNumberOfGeneratedBuffers() < num_of_buffers) {
        auto optBuf = source->receiveData();
        size_t i = 0;
        while (i*tuple_size < buffer_size - tuple_size && !!optBuf) {
            ysbRecord record(
                *((ysbRecord*) (optBuf->getBufferAs<char>() + i*tuple_size)));
            std::cout << "i=" << i << " record.ad_type: " << record.ad_type << ", record.event_type: " << record.event_type << std::endl;
            EXPECT_STREQ(record.ad_type, "banner78");
            EXPECT_TRUE(
                (!strcmp(record.event_type, "view")
                    || !strcmp(record.event_type, "click")
                    || !strcmp(record.event_type, "purchase")));
            i++;
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedTuples(), num_tuples_to_process);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), num_of_buffers);
    std::cout << "dataSuccess" << std::endl;
}

TEST_F(SourceTest, testSenseSource) {
    QueryManagerPtr queryManager = std::make_shared<QueryManager>();
    BufferManagerPtr bufferManager = std::make_shared<BufferManager>(4096, 1024);


    std::string testUDFS("...");
    createSenseSourceFuncPtr funcPtr = &createSenseSource;

    SchemaPtr schema =
        Schema::create()->addField("user_id", 16)->addField("page_id", 16)->addField(
                "campaign_id", 16)->addField("ad_type", 9)->addField("event_type", 9)
            ->addField("current_ms", UINT64)->addField("ip", INT32);

    uint64_t num_tuples_to_process = 1000;
    size_t num_of_buffers = 1000;
    uint64_t tuple_size = schema->getSchemaSizeInBytes();
    uint64_t buffer_size = num_tuples_to_process*tuple_size/num_of_buffers;
    assert(buffer_size > 0);

    const DataSourcePtr source = (*funcPtr)(schema,  bufferManager, queryManager, testUDFS);

    //TODO: please add here to code to test the setup
    std::cout << "Success" << std::endl;
}

}  //end of NES
