/*
 * BinarySource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_BINARYSOURCE_H_
#define INCLUDE_BINARYSOURCE_H_

#include "../core/DataSource.h"
#include "../core/TupleBuffer.h"
class BinarySource : public DataSource {
public:
  BinarySource(const std::string &file_path, const uint64_t &num_tuples_to_process);
  TupleBuffer receiveData();
  void fillBuffer(TupleBuffer &);

private:
  std::ifstream input;
  uint64_t num_tuples_to_process;
};

#endif /* INCLUDE_BINARYSOURCE_H_ */
