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
    NES_DEBUG("CsvFormat::writeSchema: schema is =" << schema->toString());
    outputFile.close();
    return true;
}

bool CsvFormat::writeData(TupleBuffer& inputBuffer)
{
    if(inputBuffer.getNumberOfTuples() == 0)
    {
        NES_WARNING("CsvFormat::writeData: Try to write empty buffer");
        return false;
    }

    std::ofstream outputFile;
    if (append) {
        NES_DEBUG("file appending in path=" << filePath);
        outputFile.open(filePath, std::ofstream::out | std::ofstream::app);
    } else {
        NES_DEBUG("file overwriting in path=" << filePath);
        outputFile.open(filePath, std::ofstream::out | std::ofstream::trunc);
    }
    size_t posBefore = outputFile.tellp();
    std::string bufferContent = UtilityFunctions::printTupleBufferAsCSV(inputBuffer, schema);
    outputFile << bufferContent;

    size_t posAfter = outputFile.tellp();
    outputFile.close();

    if(bufferContent.length() > 0 && posAfter > posBefore)
    {
        NES_DEBUG("CsvFormat::writeData: wrote buffer of length=" << bufferContent.length() << " successfully");
        return true;
    }
    else{
        NES_ERROR("CsvFormat::writeData: write buffer failed posBefore=" << posBefore << " posAfter=" << posAfter << " bufferContent=" << bufferContent);
        return false;
    }

}

std::string CsvFormat::toString() {
    return "CSV_FORMAT";
}
}// namespace NES