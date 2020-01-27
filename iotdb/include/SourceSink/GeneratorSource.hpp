#ifndef INCLUDE_GENERATORSOURCE_H_
#define INCLUDE_GENERATORSOURCE_H_

#include <iostream>
#include <sstream>

#include <SourceSink/DataSource.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

namespace NES {

/**
 * @brief this class implements the generator source
 * @Limitations:
 *    - This class can currently not be serialized/deserialized mostly due to the templates
 */
class GeneratorSource : public DataSource {
 public:
  /**
   * @brief constructor to create a generator source
   * @param schema of the source
   * @param number of buffer that should be processed
   * @param via template, the functor that determines what to do
   */
  GeneratorSource(const Schema &schema, const uint64_t pNum_buffers_to_process)
      : DataSource(schema) {
    this->num_buffers_to_process = pNum_buffers_to_process;
  }
  /**
   * @brief override function to create one buffer
   * @return pointer to a buffer containing the created tuples
   */
  TupleBufferPtr receiveData() override = 0;

  /**
   * @brief override the toString method for the generator source
   * @return returns string describing the generator source
   */
  const std::string toString() const override;
    SourceType getType() const override;
  protected:
  GeneratorSource() = default;
 private:
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar, const unsigned int version) {
    ar & boost::serialization::base_object<DataSource>(*this);
  }
};
/*template<typename F>
const std::string GeneratorSource<F>::toString() const {
  std::stringstream ss;
  ss << "GENERATOR_SOURCE(SCHEMA(" << schema.toString();
  ss << "), NUM_BUFFERS=" << num_buffers_to_process << "))";
  return ss.str();
}

template<typename F>
TupleBufferPtr GeneratorSource<F>::receiveData() {
  // we wait until the buffer is filled
  TupleBufferPtr buf = functor();
  generatedTuples += buf->getNumberOfTuples();
  generatedBuffers++;

  return buf;
}*/

}
#endif /* INCLUDE_GENERATORSOURCE_H_ */
