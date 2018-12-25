/*
 * BinarySource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_BINARYSOURCE_H_
#define INCLUDE_BINARYSOURCE_H_

#include <Runtime/DataSource.hpp>
#include <core/TupleBuffer.hpp>
#include <fstream>

namespace iotdb {

class BinarySource : public DataSource {
public:
  BinarySource(const Schema& schema, const std::string &file_path, const uint64_t &num_tuples_to_process);
  TupleBuffer receiveData();
  const std::string toString() const;
  void fillBuffer(TupleBuffer &);

private:
  std::ifstream input;
  const std::string file_path;
  uint64_t num_tuples_to_process;
};
}

#endif /* INCLUDE_BINARYSOURCE_H_ */
