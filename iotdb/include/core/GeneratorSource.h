/*
 * GeneratorSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_GENERATORSOURCE_H_
#define INCLUDE_GENERATORSOURCE_H_

#include "../core/DataSource.h"
#include "../core/TupleBuffer.h"

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

#endif /* INCLUDE_GENERATORSOURCE_H_ */
