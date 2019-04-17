#include <memory>
#include <sstream>
#include <string>

#include <zmq.hpp>

#include <Runtime/Dispatcher.hpp>
#include <Runtime/PrintSink.hpp>
#include <Util/Logger.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::PrintSink)

namespace iotdb {

PrintSink::PrintSink(std::ostream& pOutputStream) : DataSink(), outputStream(pOutputStream) {}

PrintSink::PrintSink(const Schema& pSchema, std::ostream& pOutputStream)
    : DataSink(pSchema), outputStream(pOutputStream)
{
}

PrintSink::~PrintSink() {}

bool PrintSink::writeData(const TupleBuffer* input_buffer)
{
    outputStream << iotdb::toString(input_buffer, this->getSchema()) << std::endl;
    return true;
}

const std::string PrintSink::toString() const
{
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema.toString() << "), ";
    return ss.str();
}

} // namespace iotdb
