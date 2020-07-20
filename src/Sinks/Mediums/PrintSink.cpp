#include <NodeEngine/QueryManager.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string>

namespace NES {

PrintSink::PrintSink(std::ostream& pOutputStream)
    : SinkMedium(),
      outputStream(pOutputStream) {
}

PrintSink::PrintSink(SchemaPtr pSchema, std::ostream& pOutputStream)
    : SinkMedium(pSchema),
      outputStream(pOutputStream) {
}

PrintSink::~PrintSink() {
}

std::string PrintSink::toString()
{
    return "PRINT_SINK";
}

bool PrintSink::writeData(TupleBuffer& input_buffer) {
    outputStream << UtilityFunctions::prettyPrintTupleBuffer(input_buffer, this->getSchema())
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

}// namespace NES
