/*
 * TupleBuffer.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */
#include <core/TupleBuffer.hpp>

TupleBuffer::TupleBuffer(void *_buffer, const uint64_t _buffer_size, const uint32_t _tuple_size_bytes,
                         const uint32_t _num_tuples)
    : buffer(_buffer), buffer_size(_buffer_size), tuple_size_bytes(_tuple_size_bytes), num_tuples(_num_tuples) {}
