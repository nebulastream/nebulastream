#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Util/Logger.hpp>
#include <iostream>
#include <Util/UtilityFunctions.hpp>
namespace NES {

CsvFormat::CsvFormat(SchemaPtr schema, std::string filePath, bool append) : SinkFormat(schema, filePath, append) {
}

bool CsvFormat::writeSchema() {
    std::ofstream outputFile;
    outputFile.open(filePath, std::ofstream::out | std::ofstream::trunc);

    std::stringstream ss;
    for (auto& f : schema->fields) {
        ss << f->toString() << ",";
    }
    outputFile << ss.str() << std::endl;
    NES_DEBUG("FileSink::writeSchema: schema is =" << ss.str());
    outputFile.close();
}

bool CsvFormat::writeData(TupleBuffer& inputBuffer)
{
    std::ofstream outputFile;
    if (append) {
        NES_DEBUG("file appending in path=" << filePath);
        outputFile.open(filePath, std::ofstream::out | std::ofstream::app);
    } else {
        NES_DEBUG("file overwriting in path=" << filePath);
        outputFile.open(filePath, std::ofstream::out | std::ios::out | std::ofstream::trunc);
    }
    std::string bufferContent = UtilityFunctions::printTupleBufferAsCSV(inputBuffer, schema);
    outputFile << bufferContent;
    outputFile.close();
}

std::string CsvFormat::getFormatAsString() {
    return "CsvFormat";
}
}// namespace NES