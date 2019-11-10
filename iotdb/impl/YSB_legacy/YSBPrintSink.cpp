#include "../../include/YSB_legacy/YSBPrintSink.hpp"

#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::YSBPrintSink)
#include <NodeEngine/Dispatcher.hpp>
#include <Util/Logger.hpp>
namespace iotdb {

struct __attribute__((packed)) ysbRecordOut {
    char campaign_id[16];
    char event_type[9];
    int64_t current_ms;
    uint32_t id;
};

YSBPrintSink::YSBPrintSink()
    : PrintSink(Schema::create()
                    .addField("user_id", 16)
                    .addField("page_id", 16)
                    .addField("campaign_id", 16)
                    .addField("event_type", 16)
                    .addField("ad_type", 16)
                    .addField("current_ms", UINT64)
                    .addField("ip", INT32),
                std::cout)
{
}

YSBPrintSink::~YSBPrintSink() {}

bool YSBPrintSink::writeData(const TupleBufferPtr input_buffer)
{

    ysbRecordOut* recordBuffer = (ysbRecordOut*)input_buffer->buffer;
    //	Schema s = Schema::create().addField("",UINT32);
    //	std::cout << iotdb::toString(input_buffer,s) << std::endl;
    for (size_t u = 0; u < input_buffer->num_tuples; u++) {
        //		std::cout << "id=" << recordBuffer[u].id << std::endl;
        //		std::cout << " ms=" << recordBuffer[u].current_ms << std::endl;
        //		std::cout << " type=" << std::string(recordBuffer[u].event_type) << std::endl;
        //		std::cout << " camp=" << std::string(recordBuffer[u].campaign_id) << std::endl;

        IOTDB_INFO("YSBPrintSink: tuple:" << u << " = "
                                          << " id=" << recordBuffer[u].id << " campaign=" << recordBuffer[u].campaign_id
                                          << " type=" << recordBuffer[u].event_type
                                          << " timestamp=" << recordBuffer[u].current_ms)

        sentTuples++;
    }
    IOTDB_INFO(" ============= YSBPrintSink: FINISHED ============")
    sentBuffer++;

    // Dispatcher::instance().releaseBuffer(input_buffer);
    return true;
}

const std::string YSBPrintSink::toString() const
{
    std::stringstream ss;
    ss << "YSB_PRINT_SINK(";
    ss << "SCHEMA(" << schema.toString() << "), ";
    return ss.str();
}

} // namespace iotdb
