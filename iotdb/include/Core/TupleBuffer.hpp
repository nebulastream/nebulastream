/*
 * TupleBuffer.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_TUPLEBUFFER_H_
#define INCLUDE_TUPLEBUFFER_H_
#include <cstdint>
#include <memory>

namespace iotdb {
class TupleBuffer;
typedef std::shared_ptr<TupleBuffer> TupleBufferPtr;
class TupleBuffer {
  public:
    TupleBuffer(void* buffer, const uint64_t buffer_size, const uint32_t tuple_size_bytes, const uint32_t num_tuples);
    TupleBuffer& operator=(const TupleBuffer&);
    void copyInto(const TupleBufferPtr&);

    void print();
    void* buffer;
    uint64_t buffer_size;
    uint64_t tuple_size_bytes;
    uint64_t num_tuples;
};

class Schema;
std::string toString(const TupleBuffer& buffer, const Schema& schema);
std::string toString(const TupleBuffer* buffer, const Schema& schema);

} // namespace iotdb
#endif /* INCLUDE_TUPLEBUFFER_H_ */
