#ifndef INCLUDE_ADAPTIVESOURCE_H_
#define INCLUDE_ADAPTIVESOURCE_H_

#include <SourceSink/DataSource.hpp>

namespace NES {

/**
 * @brief This class defines a source that adapts its sampling rate
 */
class AdaptiveSource : public DataSource {
  public:
    /**
     * Simple constructor, with initial sampling frequency
     * @param schema
     * @param bufferManager
     * @param queryManager
     */
    AdaptiveSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, size_t initialFrequency);

    /**
     * Function to sample data from a physical source
     * @return
     */
    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief Get type of source
     */
    SourceType getType() const override;

    /**
     * @brief This method is overridden here to provide adaptive schemes of sampling
     * @param bufferManager
     * @param queryManager
     */
    void runningRoutine(BufferManagerPtr, QueryManagerPtr);
};

}// namespace NES

#endif /* INCLUDE_ADAPTIVESOURCE_H_ */
