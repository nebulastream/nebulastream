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
    : DataSink(schema), printedTuples(0) {}

PrintSink::~PrintSink() { }

bool PrintSink::writeData(const std::vector<TupleBufferPtr> &input_buffers) {
	//TODO: is it really neccesary to use a vector of buffers?
	for(size_t i = 0; i < input_buffers.size(); i++)//for each buffer
	{
		IOTDB_INFO("PrintSink: Buffer No:" << i)
		writeData(input_buffers[i]);
	}
	//TODO: release buffe
}

bool PrintSink::writeData(const TupleBufferPtr input_buffer) {
	for(size_t u = 0; u < input_buffer->num_tuples; u++)
	{
		IOTDB_INFO("PrintSink: tuple:" << u << " = ")
		//TODO: how to get a tuple via a schema
		//TODO: add file heere
	}
}


bool PrintSink::writeData(const std::vector<TupleBuffer*> &input_buffers) {
	//TODO: is it really neccesary to use a vector of buffers?
	for(size_t i = 0; i < input_buffers.size(); i++)//for each buffer
	{
		IOTDB_INFO("PrintSink: Buffer No:" << i)
		writeData(input_buffers[i]);
	}
	//TODO: release buffe
}

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
	  char campaign_id[16];
	  char event_type[9];
	  int64_t current_ms;
	  uint32_t id;

};

YSBPrintSink::YSBPrintSink(const Schema& schema)
    : PrintSink(schema){}

YSBPrintSink::~YSBPrintSink() { }

bool YSBPrintSink::writeData(const std::vector<TupleBufferPtr> &input_buffers) {
	for(size_t i = 0; i < input_buffers.size(); i++)//for each buffer
	{
		IOTDB_INFO("PrintSink: Buffer No:" << i)
		writeData(input_buffers[i]);
	}
}

bool YSBPrintSink::writeData(const TupleBufferPtr input_buffer) {
	ysbRecordOut* recordBuffer = (ysbRecordOut*) input_buffer->buffer;
	IOTDB_INFO(" ============= YSBPrintSink: print buffer " << input_buffer->buffer << "============")
	for(size_t u = 0; u < input_buffer->num_tuples; u++)
	{
//		std::cout << "id=" << recordBuffer[u].id << std::endl;
//		std::cout << " ms=" << recordBuffer[u].current_ms << std::endl;
//		std::cout << " type=" << std::string(recordBuffer[u].event_type) << std::endl;
//		std::cout << " camp=" << std::string(recordBuffer[u].campaign_id) << std::endl;

		IOTDB_INFO("YSBPrintSink: tuple:" << u << " = " << " id=" << recordBuffer[u].id << " campaign=" << recordBuffer[u].campaign_id
				<< " type=" << recordBuffer[u].event_type
				<< " timestamp=" <<recordBuffer[u].current_ms)
		printedTuples++;
	}
	IOTDB_INFO(" ============= YSBPrintSink: FINISHED ============")

	Dispatcher::instance().releaseBuffer(input_buffer);
}

bool YSBPrintSink::writeData(const std::vector<TupleBuffer*> &input_buffers) {
	for(size_t i = 0; i < input_buffers.size(); i++)//for each buffer
	{
		IOTDB_INFO("PrintSink: Buffer No:" << i)
		writeData(input_buffers[i]);
	}
}

bool YSBPrintSink::writeData(const TupleBuffer* input_buffer) {
	assert(0);
	ysbRecordOut* recordBuffer = (ysbRecordOut*) input_buffer->buffer;
	IOTDB_INFO(" ============= YSBPrintSink: print buffer " << input_buffer->buffer << "============")
	for(size_t u = 0; u < input_buffer->num_tuples; u++)
	{
//		std::cout << "id=" << recordBuffer[u].id << std::endl;
//		std::cout << " ms=" << recordBuffer[u].current_ms << std::endl;
//		std::cout << " type=" << std::string(recordBuffer[u].event_type) << std::endl;
//		std::cout << " camp=" << std::string(recordBuffer[u].campaign_id) << std::endl;

		IOTDB_INFO("YSBPrintSink: tuple:" << u << " = " << " id=" << recordBuffer[u].id << " campaign=" << recordBuffer[u].campaign_id
				<< " type=" << recordBuffer[u].event_type
				<< " timestamp=" <<recordBuffer[u].current_ms)
		printedTuples++;
	}
	IOTDB_INFO(" ============= YSBPrintSink: FINISHED ============")

//	Dispatcher::instance().releaseBuffer(input_buffer);
}

const std::string YSBPrintSink::toString() const {
  std::stringstream ss;
  ss << "YSB_PRINT_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  return ss.str();
}

} // namespace iotdb
