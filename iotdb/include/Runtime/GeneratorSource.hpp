/*
 * GeneratorSource.h
 *
 *  Created on: Dec 19, 2018
 *      Author: zeuchste
 */

#ifndef INCLUDE_GENERATORSOURCE_H_
#define INCLUDE_GENERATORSOURCE_H_

#include <Runtime/DataSource.hpp>
#include <Core/TupleBuffer.hpp>
#include <iostream>
#include <sstream>

namespace iotdb {

template <typename F> class GeneratorSource : public DataSource {
public:
  GeneratorSource(const Schema& schema, const uint64_t pNum_buffers_to_process)
      : DataSource(schema), functor() {
	  this->num_buffers_to_process = pNum_buffers_to_process;
  }
  TupleBufferPtr receiveData();
  const std::string toString() const;
private:
  F functor;
};


template <typename F> TupleBufferPtr GeneratorSource<F>::receiveData() {
    //we wait until the buffer is filled
	TupleBufferPtr buf = functor();
	generatedTuples += buf->num_tuples;
    return buf;
};


template <typename F>
  const std::string GeneratorSource<F>::toString() const{
    std::stringstream ss;
    ss << "GENERATOR_SOURCE(SCHEMA(" << schema.toString();
    ss << "), NUM_BUFFERS=" << num_buffers_to_process << "))";
    return ss.str();
  }

template <typename F> class YSBGeneratorSource : public DataSource {
public:
	YSBGeneratorSource(const Schema& schema, const uint64_t pNum_buffers_to_process, size_t pCampaingCnt)
      : DataSource(schema), functor(pCampaingCnt){
	  this->num_buffers_to_process = pNum_buffers_to_process;
	  this->numberOfCampaings = pCampaingCnt;
  }
  TupleBufferPtr receiveData();
  const std::string toString() const;
private:
  F functor;
  uint64_t numberOfCampaings;
};

template <typename F> TupleBufferPtr YSBGeneratorSource<F>::receiveData() {
    //we wait until the buffer is filled
	TupleBufferPtr buf = functor();
	generatedTuples += buf->num_tuples;
    return buf;
};


template <typename F>
  const std::string YSBGeneratorSource<F>::toString() const{
    std::stringstream ss;
    ss << "YSBGeneratorSource(SCHEMA(" << schema.toString();
    ss << "), NUM_BUFFERS=" << num_buffers_to_process <<
    		" numberOfCampaings=" << numberOfCampaings << "))";
    return ss.str();
  }
};



#endif /* INCLUDE_GENERATORSOURCE_H_ */
