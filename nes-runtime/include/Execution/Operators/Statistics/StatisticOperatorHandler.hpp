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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STATISTICS_STATISTICOPERATORHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STATISTICS_STATISTICOPERATORHANDLER_HPP_

#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>

namespace NES {
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace Runtime {

class BufferManager;
using BufferManagerPtr = std::shared_ptr<BufferManager>;

namespace MemoryLayouts {
class DynamicTupleBuffer;
}

namespace Execution::Operators {
class MultiOriginWatermarkProcessor;
using MultiOriginWatermarkProcessorPtr = std::unique_ptr<MultiOriginWatermarkProcessor>;
}// namespace Execution::Operators
}// namespace Runtime

namespace Experimental::Statistics {

class StatisticOperatorHandler : public Runtime::Execution::OperatorHandler {

  public:
    /**
     * @param windowSize
     * @param slideFactor
     * @param fieldName
     * @param depth
     * @param schema
     * @param allOriginIds
     */
    StatisticOperatorHandler(uint64_t windowSize,
                             uint64_t slideFactor,
                             const std::string& logicalSourceName,
                             const std::string& fieldName,
                             uint64_t depth,
                             SchemaPtr schema,
                             const std::vector<OriginId>& allOriginIds);

    /**
     * @brief the default destructor of the abstract StatisticOperatorHandler
     */
    virtual ~StatisticOperatorHandler() = default;

    /**
     * @brief sets the bufferManager
     * @param bufferManager the bufferManager that we set
     */
    void setBufferManager(const Runtime::BufferManagerPtr& bufferManager);

    /**
     * @brief
     */
    void discardUnfinishedRemainingStatistics();

//    const Runtime::Execution::Operators::SliceAssigner& getSliceAssigner() const;
//
//    const std::vector<Runtime::TupleBuffer>& getAllStatistics() const;
//
//    const std::string& getLogicalSourceName() const;
//
//    const std::string& getFieldName() const;
//
//    uint64_t getDepth() const;
//
//    const SchemaPtr& getSchema() const;
//
//    const Runtime::Execution::Operators::MultiOriginWatermarkProcessorPtr& getWatermarkProcessor() const;
//
//    const Runtime::BufferManagerPtr& getBufferManager() const;

  private:
    Runtime::Execution::Operators::SliceAssigner sliceAssigner;
    std::vector<Runtime::TupleBuffer> allStatistics;
    std::string logicalSourceName;
    std::string fieldName;
    uint64_t depth;
    SchemaPtr schema;
    Runtime::Execution::Operators::MultiOriginWatermarkProcessorPtr watermarkProcessor;
    Runtime::BufferManagerPtr bufferManager;

    friend class CountMinOperatorHandler;
    friend class ReservoirSampleOperatorHandler;
};
}// namespace Experimental::Statistics
}// namespace NES
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STATISTICS_STATISTICOPERATORHANDLER_HPP_
