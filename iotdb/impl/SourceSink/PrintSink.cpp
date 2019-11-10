#include <SourceSink/PrintSink.hpp>
#include <memory>
#include <sstream>
#include <string>
#include <NodeEngine/Dispatcher.hpp>
#include <Util/Logger.hpp>

BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::PrintSink)

namespace iotdb {

PrintSink::PrintSink(std::ostream& pOutputStream)
    : DataSink(),
      outputStream(pOutputStream) {
}

PrintSink::PrintSink(const Schema& pSchema, std::ostream& pOutputStream)
    : DataSink(pSchema),
      outputStream(pOutputStream) {
}

PrintSink::~PrintSink() {
}

bool PrintSink::writeData(const TupleBufferPtr input_buffer) {
  outputStream << iotdb::toString(input_buffer.get(), this->getSchema())
               << std::endl;
  return true;
}

const std::string PrintSink::toString() const {
  std::stringstream ss;
  ss << "PRINT_SINK(";
  ss << "SCHEMA(" << schema.toString() << "), ";
  return ss.str();
}

void PrintSink::setup() {
// currently not required
}
void PrintSink::shutdown() {
  // currently not required
}

}  // namespace iotdb
