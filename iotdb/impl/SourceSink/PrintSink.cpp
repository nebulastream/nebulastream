#include <SourceSink/PrintSink.hpp>
#include <sstream>
#include <string>
#include <NodeEngine/Dispatcher.hpp>
#include <Util/Logger.hpp>

namespace NES {

PrintSink::PrintSink(std::ostream& pOutputStream)
    : DataSink(),
      outputStream(pOutputStream) {
}

PrintSink::PrintSink(SchemaPtr pSchema, std::ostream& pOutputStream)
    : DataSink(pSchema),
      outputStream(pOutputStream) {
}

PrintSink::~PrintSink() {
}

bool PrintSink::writeData(TupleBuffer& input_buffer) {
    outputStream << NES::toString(input_buffer, this->getSchema())
                 << std::endl;
    return true;
}

const std::string PrintSink::toString() const {
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    return ss.str();
}

void PrintSink::setup() {
    // currently not required
}
void PrintSink::shutdown() {
    // currently not required
}

SinkType PrintSink::getType() const {
    return PRINT_SINK;
}

}  // namespace NES
BOOST_CLASS_EXPORT(NES::PrintSink);
