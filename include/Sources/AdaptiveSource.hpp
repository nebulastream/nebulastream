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

#ifndef NES_INCLUDE_SOURCES_ADAPTIVE_SOURCE_HPP_
#define NES_INCLUDE_SOURCES_ADAPTIVE_SOURCE_HPP_

#include <Sources/DataSource.hpp>
#include <chrono>

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
    AdaptiveSource(SchemaPtr schema,
                   Runtime::BufferManagerPtr bufferManager,
                   Runtime::QueryManagerPtr queryManager,
                   uint64_t initialGatheringInterval,
                   OperatorId operatorId,
                   size_t numSourceLocalBuffers,
                   GatheringMode gatheringMode);

    /**
     * @brief Get type of source
     */
    SourceType getType() const override;

    /**
     * @brief This method is overridden here to provide adaptive schemes of sampling
     * @param bufferManager
     * @param queryManager
     */
    void runningRoutine() override;

    /**
     * @brief sample data and choose to update the new frequency
     * @return the filled tuple buffer
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

  private:
    /**
     * @brief sample a source, implemented by derived
     */
    virtual void sampleSourceAndFillBuffer(Runtime::TupleBuffer&) = 0;

    /**
     * @brief decision of new frequency, implemented by derived
     */
    virtual void decideNewGatheringInterval() = 0;

    std::chrono::milliseconds lastGatheringTimeStamp{};
};

using AdaptiveSourcePtr = std::shared_ptr<AdaptiveSource>;

}// namespace NES

#endif// NES_INCLUDE_SOURCES_ADAPTIVE_SOURCE_HPP_
