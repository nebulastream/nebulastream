/*
 * BinarySource.cpp
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#include <Runtime/BinarySource.hpp>
#include <fstream>
#include <Runtime/DataSource.hpp>
#include <Runtime/Dispatcher.hpp>
BinarySource::BinarySource(const std::string &_file_path, const uint64_t &_num_tuples_to_process)
    : DataSource(), input(std::ifstream(_file_path.c_str())), num_tuples_to_process(_num_tuples_to_process) {}

TupleBuffer BinarySource::receiveData() {
  TupleBuffer buf = Dispatcher::instance().getBuffer();
  fillBuffer(buf);
  return buf;
}

void BinarySource::fillBuffer(TupleBuffer &buf) {
  /* while(generated_tuples < num_tuples_to_process) */
  /* read <buf.buffer_size> bytes data from file into buffer */
  /* advance internal file pointer, if we reach the file end, set to file begin */
}
