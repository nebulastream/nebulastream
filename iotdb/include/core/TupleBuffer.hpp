/*
 * TupleBuffer.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_TUPLEBUFFER_H_
#define INCLUDE_TUPLEBUFFER_H_
#include <cstdint>

class TupleBuffer {
public:
  TupleBuffer(void *buffer, const uint64_t buffer_size, const uint32_t tuple_size_bytes, const uint32_t num_tuples);

  void *buffer;
  uint64_t buffer_size;
  uint64_t tuple_size_bytes;
  uint64_t num_tuples;
};

#endif /* INCLUDE_TUPLEBUFFER_H_ */
