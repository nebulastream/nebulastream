#include <API/Schema.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Sinks/Formats/TextFormat.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>
#include <cstring>
namespace NES {

TextFormat::TextFormat(SchemaPtr schema, BufferManagerPtr bufferManager) : SinkFormat(schema, bufferManager) {

}

std::optional<TupleBuffer> TextFormat::getSchema() {
    //noting to do as this is part of pretty print
    return std::nullopt;
}

std::vector<TupleBuffer> TextFormat::getData(TupleBuffer& inputBuffer) {
    std::vector<TupleBuffer> buffers;

    if (inputBuffer.getNumberOfTuples() == 0) {
        NES_WARNING("TextFormat::getData: Try to write empty buffer");
        return buffers;
    }
    std::string bufferContent = UtilityFunctions::prettyPrintTupleBuffer(inputBuffer, schema);
    size_t contentSize = bufferContent.length();
    NES_DEBUG("TextFormat::getData content size=" << contentSize << " content=" << bufferContent);

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
        std::memcpy(buf.getBuffer(), bufferContent.c_str(), contentSize);
        buf.setNumberOfTuples(contentSize);
        buffers.push_back(buf);
        return buffers;
    }

}

std::string TextFormat::toString() {
    return "TEXT_FORMAT";
}

SinkFormats TextFormat::getSinkFormat() {
    return TEXT_FORMAT;
}

}// namespace NES