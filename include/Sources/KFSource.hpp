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
#ifndef NES_KF_SOURCE_HPP_
#define NES_KF_SOURCE_HPP_

#include <Runtime/BufferRecycler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/GeneratorSource.hpp>

namespace NES {

namespace Runtime {
namespace detail {
class MemorySegment;
}// namespace detail
}// namespace Runtime

class KFSource : public GeneratorSource, public Runtime::BufferRecycler {

  public:

    explicit KFSource(SchemaPtr schema,
                      const std::shared_ptr<uint8_t>& memoryArea,
                      size_t memoryAreaSize,
                      Runtime::BufferManagerPtr bufferManager,
                      Runtime::QueryManagerPtr queryManager,
                      uint64_t numBuffersToProcess,
                      uint64_t gatheringValue,
                      OperatorId operatorId,
                      size_t numSourceLocalBuffers,
                      GatheringMode gatheringMode,
                      uint64_t sourceAffinity,
                      std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors);
    /**
     * @brief Provides the type of the source
     * @return the type of the source
     */
    SourceType getType() const override;

    /**
     * @brief read from path and load in memory
     */
    void open() override;

    /**
     * @brief runningRoutine with KF
     */
    void runningRoutine() override{};

    /**
     * @brief This method is implemented only to comply with the API: it will crash the system if called.
     * @return a nullopt
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
     * @brief Interface method for pooled/unpooled buffer recycling
     */
    void recyclePooledBuffer(Runtime::detail::MemorySegment*) override{};
    void recycleUnpooledBuffer(Runtime::detail::MemorySegment*) override{};

  protected:
    std::shared_ptr<uint8_t> memoryArea;
    const size_t memoryAreaSize;
    uint64_t bufferSize;
    Runtime::TupleBuffer numaLocalMemoryArea;
};

}// namespace NES
#endif//NES_KF_SOURCE_HPP_
