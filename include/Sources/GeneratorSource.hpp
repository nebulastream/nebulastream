#ifndef INCLUDE_GENERATORSOURCE_H_
#define INCLUDE_GENERATORSOURCE_H_

#include <iostream>
#include <sstream>

#include <Sources/DataSource.hpp>

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
    GeneratorSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const uint64_t numbersOfBufferToProduce)
        : DataSource(schema, bufferManager, queryManager) {
        this->numBuffersToProcess = numbersOfBufferToProduce;
    }
    /**
   * @brief override function to create one buffer
   * @return pointer to a buffer containing the created tuples
   */
    virtual std::optional<TupleBuffer> receiveData() = 0;

    /**
     * @brief override the toString method for the generator source
     * @return returns string describing the generator source
     */
    const std::string toString() const override;
    SourceType getType() const override;

  protected:
    GeneratorSource() = default;
};

}// namespace NES
#endif /* INCLUDE_GENERATORSOURCE_H_ */
