#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <Runtime/PrintSink.hpp>
#include <Util/Logger.hpp>
#include <Runtime/Dispatcher.hpp>
namespace iotdb {

PrintSink::PrintSink(const Schema &schema)
    : DataSink(schema) {}

PrintSink::~PrintSink() { }

bool PrintSink::writeData(const TupleBuffer* input_buffer) {
	for(size_t u = 0; u < input_buffer->num_tuples; u++)
	{
		IOTDB_INFO("PrintSink: tuple:" << u << " = ")
		//TODO: how to get a tuple via a schema
		//TODO: add file heere
	}
}


const std::string PrintSink::toString() const {
  std::stringstream ss;
  ss << "PRINT_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  return ss.str();
}

struct __attribute__((packed)) ysbRecordOut {
	  uint8_t campaign_id[16];
	  char event_type[9];
	  int64_t current_ms;
};

YSBPrintSink::YSBPrintSink(const Schema& schema)
    : PrintSink(schema) {}

YSBPrintSink::~YSBPrintSink() { }

bool YSBPrintSink::writeData(const TupleBuffer* input_buffer) {
	ysbRecordOut* recordBuffer = (ysbRecordOut*) input_buffer;
	for(size_t u = 0; u < input_buffer->num_tuples; u++)
	{
		IOTDB_INFO("PrintSink: tuple:" << u << " = " << " campaign=" << recordBuffer[u].campaign_id
				<< " type=" << recordBuffer[u].event_type
				<< " timestamp=" <<recordBuffer[u].current_ms)
	}

	//Dispatcher::instance().releaseBuffer(input_buffer);
}

const std::string YSBPrintSink::toString() const {
  std::stringstream ss;
  ss << "YSB_PRINT_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  return ss.str();
}

} // namespace iotdb
