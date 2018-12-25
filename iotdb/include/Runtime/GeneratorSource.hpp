/*
 * GeneratorSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_GENERATORSOURCE_H_
#define INCLUDE_GENERATORSOURCE_H_

#include <Runtime/DataSource.hpp>
#include <core/TupleBuffer.hpp>
#include <iostream>

namespace iotdb{

template <typename F> class GeneratorSource : public DataSource {
public:
  GeneratorSource(const uint64_t &_num_tuples_to_process)
      : functor(), num_tuples_to_process(_num_tuples_to_process), generated_tuples(0) {}
  TupleBuffer receiveData();

private:
  F functor;
  uint64_t num_tuples_to_process;
  uint64_t generated_tuples;
};

template <typename F> TupleBuffer GeneratorSource<F>::receiveData() {
  std::cout << "produced " << generated_tuples << " tuples from total " << num_tuples_to_process << " tuples "
            << std::endl;

  if (generated_tuples < num_tuples_to_process) {
    TupleBuffer buf = functor(generated_tuples, num_tuples_to_process);
    generated_tuples += buf.num_tuples;
    return buf;
  }
  /* job done, quit this source */
  // std::this_thread::sleep_for(std::chrono::seconds(1)); //nanoseconds(100000));
  return TupleBuffer(NULL, 0, 0, 0);
}
}

#endif /* INCLUDE_GENERATORSOURCE_H_ */
