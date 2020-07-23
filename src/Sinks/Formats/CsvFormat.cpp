#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <sstream>
#include <cstring><cstring>
namespace NES {

CsvFormat::CsvFormat(SchemaPtr schema, BufferManagerPtr bufferManager) : SinkFormat(schema, bufferManager) {
}

std::optional<TupleBuffer> CsvFormat::getSchema() {
    auto buf = this->bufferManager->getBufferBlocking();
    std::stringstream ss;
    size_t numberOfFields = schema->fields.size();
    for (size_t i = 0; i < numberOfFields; i++) {
        ss << schema->fields[i]->toString();
        if(i < numberOfFields-1)
        {
            ss << ",";
        }
    }

    ss << std::endl;
    ss.seekg(0, std::ios::end);
    if(ss.tellg() > buf.getBufferSize())
    {
        NES_THROW_RUNTIME_ERROR("Schema buffer is too large");
    }
    std::string schemaString = ss.str();
    std::memcpy(buf.getBuffer(), schemaString.c_str(), ss.tellg());
    buf.setNumberOfTuples(ss.tellg());
    return buf;
}

std::vector<TupleBuffer> CsvFormat::getData(TupleBuffer& inputBuffer) {
    std::vector<TupleBuffer> buffers;

    if (inputBuffer.getNumberOfTuples() == 0) {
        NES_WARNING("CsvFormat::getData: Try to write empty buffer");
        return buffers;
    }
    std::string bufferContent = UtilityFunctions::printTupleBufferAsCSV(inputBuffer, schema);
    size_t contentSize = bufferContent.length();
    if(inputBuffer.getBufferSize() < contentSize)
    {
        NES_DEBUG("CsvFormat::getData: content is larger than one buffer");
        size_t numberOfBuffers = contentSize / inputBuffer.getBufferSize();
        for (size_t i = 0; i < numberOfBuffers; i++)
        {
            std::string copyString = bufferContent.substr(0, contentSize);
            bufferContent = bufferContent.substr(contentSize, bufferContent.length()- contentSize);
            NES_DEBUG("CsvFormat::getData: copy string=" << copyString << " new content=" << bufferContent);
            auto buf = this->bufferManager->getBufferBlocking();
            std::copy(copyString.begin(), copyString.end(), buf.getBuffer());
            buf.setNumberOfTuples(contentSize);
            buffers.push_back(buf);
        }
        NES_DEBUG("CsvFormat::getData: successfully copied buffer=" <<  numberOfBuffers);

    }
    else{
        NES_DEBUG("CsvFormat::getData: content fits in one buffer");
        auto buf = this->bufferManager->getBufferBlocking();
        std::memcpy(buf.getBufferAs<char>(), bufferContent.c_str(), contentSize);
        buf.setNumberOfTuples(contentSize);
        buffers.push_back(buf);
        return buffers;
    }
}

std::string CsvFormat::toString() {
    return "CSV_FORMAT";
}
}// namespace NES