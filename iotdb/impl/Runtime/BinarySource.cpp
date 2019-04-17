/*
 * BinarySource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <Runtime/BinarySource.hpp>
#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
#include <assert.h>
#include <fstream>
#include <sstream>

namespace iotdb {

BinarySource::BinarySource(const Schema& schema, const std::string& _file_path, const uint64_t& _num_tuples_to_process)
    : DataSource(schema), input(std::ifstream(_file_path.c_str())), file_path(_file_path),
      num_tuples_to_process(_num_tuples_to_process)
{
}

TupleBufferPtr BinarySource::receiveData()
{
    assert(0); // not implemented yet
    TupleBufferPtr buf = BufferManager::instance().getBuffer();
    //  fillBuffer(buf);
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
}
} // namespace iotdb
