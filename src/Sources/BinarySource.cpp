#include <NodeEngine/QueryManager.hpp>
#include <assert.h>
#include <sstream>

#include <Sources/BinarySource.hpp>
#include <Sources/DataSource.hpp>
#include <Util/Logger.hpp>

namespace NES {

BinarySource::BinarySource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const std::string& _file_path)
    : DataSource(schema, bufferManager, queryManager),
      input(std::ifstream(_file_path.c_str())),
      file_path(_file_path) {
    input.seekg(0, input.end);
    file_size = input.tellg();
    if (file_size == -1) {
        NES_ERROR("ERROR: File " << _file_path << " is corrupted");
        assert(0);
    }
    input.seekg(0, input.beg);
    tuple_size = schema->getSchemaSizeInBytes();
}

std::optional<TupleBuffer> BinarySource::receiveData() {
    auto buf = this->bufferManager->getBufferBlocking();
    fillBuffer(buf);
    buf.setWaterMark(this->waterMark->getWaterMark());
    return buf;
}

const std::string BinarySource::toString() const {
    std::stringstream ss;
    ss << "BINARY_SOURCE(SCHEMA(" << schema->toString() << "), FILE=" << file_path
       << ")";
    return ss.str();
}

void BinarySource::fillBuffer(TupleBuffer& buf) {
    /** while(generated_tuples < num_tuples_to_process)
     * read <buf.buffer_size> bytes data from file into buffer
     * advance internal file pointer, if we reach the file end, set to file begin
     */

    if (input.tellg() == file_size) {
        input.seekg(0, input.beg);
    }
    size_t size_to_read =
        buf.getBufferSize() < (uint64_t) file_size ? buf.getBufferSize() : file_size;
    input.read(buf.getBufferAs<char>(), size_to_read);
    uint64_t generated_tuples_this_pass = size_to_read / tuple_size;
    buf.setNumberOfTuples(generated_tuples_this_pass);

    generatedTuples += generated_tuples_this_pass;
    generatedBuffers++;
}
SourceType BinarySource::getType() const {
    return BINARY_SOURCE;
}

const std::string& BinarySource::getFilePath() const {
    return file_path;
}
}// namespace NES
