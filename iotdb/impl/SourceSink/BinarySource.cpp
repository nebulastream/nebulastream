/*
 * BinarySource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include "../../include/SourceSink/BinarySource.hpp"

#include <NodeEngine/Dispatcher.hpp>
#include <assert.h>
#include <fstream>
#include <sstream>
#include "../../include/SourceSink/DataSource.hpp"

namespace iotdb {

BinarySource::BinarySource(const Schema& schema, const std::string& _file_path, const uint64_t& _num_tuples_to_process)
    : DataSource(schema), input(std::ifstream(_file_path.c_str())), file_path(_file_path),
      num_tuples_to_process(_num_tuples_to_process)
{
    input.seekg(0, input.end);
    file_size = input.tellg();
    if (file_size == -1) {
        std::cerr << "ERROR: File " << _file_path << " is corrupted" << std::endl;
        std::abort();
    }
    input.seekg(0, input.beg);
    tuple_size = schema.getSchemaSize();
    generatedTuples = 0;
    generatedBuffers = 0;
}

TupleBufferPtr BinarySource::receiveData()
{
    TupleBufferPtr buf = BufferManager::instance().getBuffer();
    fillBuffer(*buf);
    return buf;
}

const std::string BinarySource::toString() const
{
    std::stringstream ss;
    ss << "BINARY_SOURCE(SCHEMA(" << schema.toString() << "), FILE=" << file_path << ")";
    ss << ", NUM_TUPLES=" << num_tuples_to_process << "))";
    return ss.str();
}

void BinarySource::fillBuffer(TupleBuffer& buf)
{
    /* while(generated_tuples < num_tuples_to_process) */
    /* read <buf.buffer_size> bytes data from file into buffer */
    /* advance internal file pointer, if we reach the file end, set to file begin */

    if (input.tellg() == file_size) {
        input.seekg(0, input.beg);
    }
    size_t size_to_read = buf.buffer_size < (uint64_t)file_size ? buf.buffer_size : file_size;
    input.read((char *)buf.buffer, size_to_read);
    uint64_t generated_tuples_this_pass = size_to_read / tuple_size;
    buf.tuple_size_bytes = tuple_size;
    buf.num_tuples = generated_tuples_this_pass;

    generatedTuples += generated_tuples_this_pass;
    generatedBuffers ++;
}
} // namespace iotdb
