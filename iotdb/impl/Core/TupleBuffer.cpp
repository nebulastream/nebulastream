/*
 * TupleBuffer.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
#include <Core/TupleBuffer.hpp>
BOOST_CLASS_EXPORT_IMPLEMENT(iotdb::TupleBuffer);
#include <cstring>
#include <iostream>
namespace iotdb {

TupleBuffer::TupleBuffer(void* _buffer, const uint64_t _buffer_size, const uint32_t _tuple_size_bytes,
                         const uint32_t _num_tuples)
    : buffer(_buffer), buffer_size(_buffer_size), tuple_size_bytes(_tuple_size_bytes), num_tuples(_num_tuples)
{
}

void TupleBuffer::copyInto(const TupleBufferPtr& other)
{
    if (other && other.get() != this) {
        this->buffer_size = other->buffer_size;
        this->tuple_size_bytes = other->tuple_size_bytes;
        this->num_tuples = other->num_tuples;
        std::memcpy(this->buffer, other->buffer, other->buffer_size);
    }
}

TupleBuffer& TupleBuffer::operator=(const TupleBuffer& other)
{
    if (this != &other) {
        this->buffer_size = other.buffer_size;
        this->tuple_size_bytes = other.tuple_size_bytes;
        this->num_tuples = other.num_tuples;
        std::memcpy(this->buffer, other.buffer, other.buffer_size);
    }
    return *this;
}

void TupleBuffer::print()
{
    std::cout << "buffer address=" << buffer << std::endl;
    std::cout << "buffer size=" << buffer_size << std::endl;
    std::cout << "buffer tuple_size_bytes=" << tuple_size_bytes << std::endl;
    std::cout << "buffer num_tuples=" << num_tuples << std::endl;
}
} // namespace iotdb
