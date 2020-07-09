#ifndef INCLUDE_ADAPTIVESOURCE_H_
#define INCLUDE_ADAPTIVESOURCE_H_

#include <SourceSink/DataSource.hpp>

namespace NES {
class TupleBuffer;

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
    AdaptiveSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager,
                   size_t initialGatheringInterval);

    /**
     * @brief Get type of source
     */
    SourceType getType() const override;

    /**
     * @brief This method is overridden here to provide adaptive schemes of sampling
     * @param bufferManager
     * @param queryManager
     */
    void runningRoutine(BufferManagerPtr, QueryManagerPtr) override;

    /**
     * @brief sample data and choose to update the new frequency
     * @return the filled tuple buffer
     */
    std::optional<TupleBuffer> receiveData() override;

  private:
    /**
     * @brief sample a source, implemented by derived
     */
    virtual void sampleSourceAndFillBuffer(TupleBuffer&) = 0;

    /**
     * @brief decision of new frequency, implemented by derived
     */
    virtual void decideNewGatheringInterval() = 0;
};

typedef std::shared_ptr<AdaptiveSource> AdaptiveSourcePtr;

}// namespace NES

#endif /* INCLUDE_ADAPTIVESOURCE_H_ */
