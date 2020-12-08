/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef INCLUDE_ADAPTIVESOURCE_H_
#define INCLUDE_ADAPTIVESOURCE_H_

#include <Sources/DataSource.hpp>

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
    AdaptiveSource(SchemaPtr schema, BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                   uint64_t initialGatheringInterval, OperatorId operatorId);

    /**
     * @brief Get type of source
     */
    SourceType getType() const override;

    /**
     * @brief This method is overridden here to provide adaptive schemes of sampling
     * @param bufferManager
     * @param queryManager
     */
    void runningRoutine(BufferManagerPtr, NodeEngine::QueryManagerPtr) override;

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

    uint64_t lastGatheringTimeStamp;
};

typedef std::shared_ptr<AdaptiveSource> AdaptiveSourcePtr;

}// namespace NES

#endif /* INCLUDE_ADAPTIVESOURCE_H_ */
