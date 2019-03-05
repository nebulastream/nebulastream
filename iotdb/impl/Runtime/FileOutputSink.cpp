#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <zmq.hpp>

#include <Runtime/FileOutputSink.hpp>
#include <Util/Logger.hpp>

namespace iotdb {

FileOutputSink::FileOutputSink(const Schema &schema)
    : DataSink(schema) {}
FileOutputSink::~FileOutputSink() { }

bool FileOutputSink::writeData(const std::vector<TupleBuffer *> &input_buffers) {
	//TODO: is it really neccesary to use a vector of buffers?
	for(size_t i = 0; i < input_buffers.size(); i++)//for each buffer
	{
		IOTDB_INFO("PrintSink: Buffer No:" << i)
		TupleBuffer* currentBuffer = input_buffers[i];
		for(size_t u = 0; u < currentBuffer->num_tuples; u++)
		{
			IOTDB_INFO("PrintSink: tuple:" << u << " = ")
			//TODO: how to get a tuple via a schema
		}
	}
}

const std::string FileOutputSink::toString() const {
  std::stringstream ss;
  ss << "PRINT_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  return ss.str();
}


} // namespace iotdb
