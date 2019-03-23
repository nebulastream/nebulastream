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

#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/shared_ptr.hpp>
#include <boost/serialization/base_object.hpp>

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
	generatedBuffers++;

    return buf;
};


template <typename F>
  const std::string GeneratorSource<F>::toString() const{
    std::stringstream ss;
    ss << "GENERATOR_SOURCE(SCHEMA(" << schema.toString();
    ss << "), NUM_BUFFERS=" << num_buffers_to_process << "))";
    return ss.str();
  }


};
//#include <boost/serialization/export.hpp>
//#include <boost/archive/text_iarchive.hpp>
//#include <boost/archive/text_oarchive.hpp>
//BOOST_CLASS_EXPORT_KEY(iotdb::YSBGeneratorSource);


#endif /* INCLUDE_GENERATORSOURCE_H_ */
