#include <cassert>
#include <iostream>
#include <memory>

#include <Runtime/DataSink.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::DataSink)
#include <Runtime/YSBPrintSink.hpp>

#include <Runtime/PrintSink.hpp>
#include <Runtime/ZmqSink.hpp>
#include <Util/ErrorHandling.hpp>
#include <Util/Logger.hpp>

namespace iotdb {

DataSink::DataSink(const Schema& _schema) : schema(_schema), processedBuffer(0), processedTuples(0)
{
    std::cout << "Init Data Sink!" << std::endl;
}

const Schema& DataSink::getSchema() const { return schema; }

bool DataSink::writeData(const std::vector<TupleBuffer*>& input_buffers)
{
    for (const auto& buf : input_buffers) {
        if (!writeData(buf)) {
            return false;
        }
    }
    return true;
}

bool DataSink::writeData(const std::vector<TupleBufferPtr>& input_buffers)
{
    std::vector<TupleBuffer*> buffs;
    for (const auto& buf : input_buffers) {
        buffs.push_back(buf.get());
    }
    return writeData(buffs);
}

bool DataSink::writeData(const TupleBufferPtr input_buffer) { return writeData(input_buffer.get()); }

DataSink::~DataSink() { IOTDB_DEBUG("Destroy Data Sink  " << this) }

const DataSinkPtr createTestSink()
{
    // instantiate TestSink
    IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSinkPtr createYSBPrintSink() { return std::make_shared<YSBPrintSink>(); }

const DataSinkPtr createBinaryFileSink(const Schema& schema, const std::string& path_to_file)
{
    // instantiate BinaryFileSink
    IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSinkPtr createRemoteTCPSink(const Schema& schema, const std::string& server_ip, int port)
{
    // instantiate RemoteSocketSink
    IOTDB_FATAL_ERROR("Called unimplemented Function");
}

const DataSinkPtr createZmqSink(const Schema& schema, const std::string& host, const uint16_t port)
{
    return std::make_shared<ZmqSink>(schema, host, port);
}

} // namespace iotdb
