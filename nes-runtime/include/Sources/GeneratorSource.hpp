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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_GENERATORSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_GENERATORSOURCE_HPP_

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
     * @param schema of the data that this source produces
     * @param bufferManager pointer to the buffer manager
     * @param queryManager pointer to the query manager
     * @param numberOfBuffersToProduce the number of buffers to be produced by the source
     * @param operatorId current operator id
     * @param originId represents the identifier of the upstream operator that represents the origin of the input stream
     * @param statisticId represents the unique identifier of components that we can track statistics for
     * @param numSourceLocalBuffers the number of buffers allocated to a source
     * @param gatheringMode the gathering mode (INTERVAL_MODE, INGESTION_RATE_MODE, or ADAPTIVE_MODE)
     * @param physicalSourceName the name and unique identifier of a physical source
     * @param successors the subsequent operators in the pipeline to which the data is pushed
     */
    GeneratorSource(SchemaPtr schema,
                    Runtime::BufferManagerPtr bufferManager,
                    Runtime::QueryManagerPtr queryManager,
                    uint64_t numberOfBufferToProduce,
                    OperatorId operatorId,
                    OriginId originId,
                    StatisticId statisticId,
                    size_t numSourceLocalBuffers,
                    GatheringMode gatheringMode,
                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors,
                    const std::string& physicalSourceName = std::string("defaultPhysicalStreamName"))
        : DataSource(std::move(schema),
                     std::move(bufferManager),
                     std::move(queryManager),
                     operatorId,
                     originId,
                     statisticId,
                     numSourceLocalBuffers,
                     gatheringMode,
                     physicalSourceName,
                     false,
                     false,
                     std::move(successors)) {
        this->numberOfBuffersToProduce = numberOfBufferToProduce;
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
#endif// NES_RUNTIME_INCLUDE_SOURCES_GENERATORSOURCE_HPP_
