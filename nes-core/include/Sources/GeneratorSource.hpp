/*
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

#ifndef NES_INCLUDE_SOURCES_GENERATORSOURCE_HPP_
#define NES_INCLUDE_SOURCES_GENERATORSOURCE_HPP_

#include <iostream>
#include <sstream>

#include <Sources/DataSource.hpp>
#include <utility>

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
    GeneratorSource(SchemaPtr schema,
                    Runtime::BufferManagerPtr bufferManager,
                    Runtime::QueryManagerPtr queryManager,
                    uint64_t numbersOfBufferToProduce,
                    OperatorId operatorId,
                    size_t numSourceLocalBuffers,
                    GatheringMode::Value gatheringMode,
                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : DataSource(std::move(schema),
                     std::move(bufferManager),
                     std::move(queryManager),
                     operatorId,
                     numSourceLocalBuffers,
                     gatheringMode,
                     std::move(successors)) {
        this->numBuffersToProcess = numbersOfBufferToProduce;
    }
    /**
   * @brief override function to create one buffer
   * @return pointer to a buffer containing the created tuples
   */
    std::optional<Runtime::TupleBuffer> receiveData() override = 0;

    /**
     * @brief override the toString method for the generator source
     * @return returns string describing the generator source
     */
    std::string toString() const override;
    SourceType getType() const override;
};

}// namespace NES
#endif  // NES_INCLUDE_SOURCES_GENERATORSOURCE_HPP_
