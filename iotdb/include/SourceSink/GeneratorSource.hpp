#ifndef INCLUDE_GENERATORSOURCE_H_
#define INCLUDE_GENERATORSOURCE_H_

#include <Core/TupleBuffer.hpp>
#include <iostream>
#include <sstream>

#include <SourceSink/DataSource.hpp>

namespace iotdb {

/**
 * @brief this class implements the generator source
 * @Limitations:
 *    - This class can currently not be serialized/deserialized mostly due to the templates
 */
template<typename F> class GeneratorSource : public DataSource {
 public:
  /**
   * @brief constructor to create a generator source
   * @param schema of the source
   * @param number of buffer that should be processed
   * @param via template, the functor that determines what to do
   */
  GeneratorSource(const Schema& schema, const uint64_t pNum_buffers_to_process)
      : DataSource(schema),
        functor() {
    this->num_buffers_to_process = pNum_buffers_to_process;
  }
  /**
   * @brief override function to create one buffer
   * @return pointer to a buffer containing the created tuples
   */
  TupleBufferPtr receiveData();

  /**
   * @brief override the toString method for the generator source
   * @return returns string describing the generator source
   */
  const std::string toString() const;

 private:
  //the functor that is applied to create one buffer
  F functor;

  GeneratorSource(){};
};



template<typename F> const std::string GeneratorSource<F>::toString() const {
  std::stringstream ss;
  ss << "GENERATOR_SOURCE(SCHEMA(" << schema.toString();
  ss << "), NUM_BUFFERS=" << num_buffers_to_process << "))";
  return ss.str();
}

template<typename F>
TupleBufferPtr GeneratorSource<F>::receiveData() {
  // we wait until the buffer is filled
  TupleBufferPtr buf = functor();
  generatedTuples += buf->num_tuples;
  generatedBuffers++;

  return buf;
}
}
#endif /* INCLUDE_GENERATORSOURCE_H_ */
