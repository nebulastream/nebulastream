#include "gtest/gtest.h"

#include <iostream>
#include <string>
#include <cstring>
#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <Runtime/GeneratorSource.hpp>
#include <Runtime/ThreadPool.hpp>
#include <Runtime/BufferManager.hpp>

#include <Core/DataTypes.hpp>

#include <Util/Logger.hpp>
namespace iotdb {

struct __attribute__((packed)) ysbRecord {
    char user_id[16];
    char page_id[16];
    char campaign_id[16];
    char ad_type[9];
    char event_type[9];
    int64_t current_ms;
    uint32_t ip;

    ysbRecord() {
        event_type[0] = '-'; // invalid record
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
}; // size 78 bytes

typedef const DataSourcePtr (*createFileSourceFuncPtr)(const Schema &, const std::string &);

int test(createFileSourceFuncPtr funcPtr, std::string& path_to_file) {
    Schema schema = Schema::create()
        .addField("user_id", 16)
        .addField("page_id", 16)
        .addField("campaign_id", 16)
        .addField("ad_type", 9)
        .addField("event_type", 9)
        .addField("current_ms", UINT64)
        .addField("ip", INT32);

    uint64_t num_tuples_to_process = 1000;
    size_t num_of_buffers = 1000;
    uint64_t tuple_size = schema.getSchemaSize();
    uint64_t buffer_size = num_tuples_to_process * tuple_size / num_of_buffers;
    assert(buffer_size > 0);
    BufferManager::instance().setNumberOfBuffers(0);
    BufferManager::instance().setNumberOfBuffers(num_of_buffers);
    BufferManager::instance().setBufferSize(buffer_size);

    const DataSourcePtr source = (*funcPtr)(schema, path_to_file);

    while (source->getNumberOfGeneratedBuffers() < num_of_buffers) {
        TupleBufferPtr buf = source->receiveData();
        size_t i = 0;
        while (i * tuple_size < buffer_size) {
            ysbRecord record(*((ysbRecord *)((char *)buf->buffer + i*tuple_size)));
            // std::cout << "record.ad_type: " << record.ad_type << ", record.event_type: " << record.event_type << std::endl;
            EXPECT_STREQ(record.ad_type, "banner78");
            EXPECT_TRUE((! strcmp(record.event_type, "view") ||
                         ! strcmp(record.event_type, "click") ||
                         ! strcmp(record.event_type, "purchase") ));
            i ++;
        }
    }

    EXPECT_EQ(source->getNumberOfGeneratedTuples(), num_tuples_to_process);
    EXPECT_EQ(source->getNumberOfGeneratedBuffers(), num_of_buffers);
    std::cout << "Success" << std::endl;
    return 0;
}

int testBinarySource() {
    std::string path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.bin";
    createFileSourceFuncPtr funcPtr = &createBinaryFileSource;
    test(funcPtr, path_to_file);
    return 0;
}

int testCSVSource() {
    std::string path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.csv";
    createFileSourceFuncPtr funcPtr = &createCSVFileSource;
    test(funcPtr, path_to_file);
    return 0;
}
}

int main(int argc, const char* argv[]) {
    setupLogger();
    iotdb::Dispatcher::instance();
    iotdb::BufferManager::instance();
    iotdb::testBinarySource();
    iotdb::testCSVSource();
    return 0;
}
