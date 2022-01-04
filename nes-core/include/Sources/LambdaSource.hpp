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

#ifndef NES_INCLUDE_SOURCES_LAMBDASOURCE_HPP_
#define NES_INCLUDE_SOURCES_LAMBDASOURCE_HPP_
#include <Sources/DataSource.hpp>
#include <Sources/GeneratorSource.hpp>
#include <chrono>

namespace NES::Runtime {
class TupleBuffer;
}

namespace NES {

class LambdaSource : public GeneratorSource {
  public:
    /**
   * @brief The constructor of a Lambda Source
   * @param schema the schema of the source
   * @param bufferManager valid pointer to the buffer manager
   * @param queryManager valid pointer to the query manager
     *
   * @param operatorId the valid id of the source
   */
    explicit LambdaSource(
        SchemaPtr schema,
        Runtime::BufferManagerPtr bufferManager,
        Runtime::QueryManagerPtr queryManager,
        uint64_t numbersOfBufferToProduce,
        uint64_t gatheringValue,
        std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
        OperatorId operatorId,
        size_t numSourceLocalBuffers,
        GatheringMode::Value gatheringMode,
        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
        uint64_t sourceAffinity);

    SourceType getType() const override;

    std::optional<Runtime::TupleBuffer> receiveData() override;

    /**
    * @brief Provides a string representation of the source
    * @return The string representation of the source
    */
    std::string toString() const override;
    /**
     * @brief method to stop the source.
     * 1.) check if bool running is false, if false return, if not stop source
     * 2.) stop thread by join
     */
    bool stop(bool graceful) override;

  protected:
    uint64_t numberOfTuplesToProduce;
    std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)> generationFunction;
};

using LambdaSourcePtr = std::shared_ptr<LambdaSource>;

}// namespace NES
#endif  // NES_INCLUDE_SOURCES_LAMBDASOURCE_HPP_
