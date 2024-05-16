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

#ifndef NES_NES_RUNTIME_INCLUDE_SOURCES_QUERYSTATISTICSOURCE_HPP_
#define NES_NES_RUNTIME_INCLUDE_SOURCES_QUERYSTATISTICSOURCE_HPP_

#include <Sources/DataSource.hpp>
#include <list>

namespace NES {

namespace Runtime {
class TupleBuffer;
}


class QueryStatisticSource : public DataSource {
  public:
    QueryStatisticSource(const SchemaPtr& schema,
                         const Runtime::BufferManagerPtr& bufferManager,
                         const Runtime::QueryManagerPtr& queryManager,
                         const OperatorId& operatorId,
                         const OriginId& originId,
                         StatisticId statisticId,
                         size_t numSourceLocalBuffers,
                         GatheringMode gatheringMode,
                         const std::string& physicalSourceName,
                         const std::vector<Runtime::Execution::SuccessorExecutablePipeline>& executableSuccessors,
                         uint64_t sourceAffinity,
                         uint64_t taskQueueId);

    /**
     * @brief Writes the statistics of the query plans into a buffer. If there is not enough space in the tuple buffer,
     * we write them to a vector (leftOverQueryPlanIds) and try to write them in the next call.
     * @return Buffer or a nullopt
     */
    std::optional<Runtime::TupleBuffer> receiveData() override;
    std::string toString() const override;
    SourceType getType() const override;

  private:
    void writeStatisticIntoBuffer(Runtime::TupleBuffer buffer, const Runtime::QueryStatistics& statistic);


    // Maybe we just ensure that the field in the schema of the memory layout are in a specific order. Then, we would not
    // have to store all fieldNames here
    const std::string processedTasksFieldName;
    const std::string timestampLastProcessedTaskFieldName;
    const std::string processedTuplesFieldName;
    const std::string processedBuffersFieldName;
    const std::string processedWatermarksFieldName;
    const std::string latencySumFieldName;
    const std::string queueSizeSumFieldName;
    const std::string availableGlobalBufferSumFieldName;
    const std::string availableFixedBufferSumFieldName;
    const std::string timestampFirstProcessedTaskFieldName;

    std::list<DecomposedQueryPlanId> nextQueryPlanIds;

};

}// namespace NES

#endif//NES_NES_RUNTIME_INCLUDE_SOURCES_QUERYSTATISTICSOURCE_HPP_
