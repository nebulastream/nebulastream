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

#include <Execution/Operators/Statistics/StatisticOperatorHandler.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Experimental::Statistics {

StatisticOperatorHandler::StatisticOperatorHandler(uint64_t windowSize,
                                                   uint64_t slideFactor,
                                                   const std::string& logicalSourceName,
                                                   const std::string& fieldName,
                                                   uint64_t depth,
                                                   NES::SchemaPtr schema,
                                                   const std::vector<OriginId>& allOriginIds)
    : sliceAssigner(windowSize, slideFactor), logicalSourceName(logicalSourceName), fieldName(fieldName), depth(depth),
      schema(std::move(schema)),
      watermarkProcessor(std::make_unique<Runtime::Execution::Operators::MultiOriginWatermarkProcessor>(allOriginIds)) {}

void StatisticOperatorHandler::discardUnfinishedRemainingStatistics() { allStatistics.clear(); }

//const Runtime::Execution::Operators::SliceAssigner& StatisticOperatorHandler::getSliceAssigner() const { return sliceAssigner; }
//
//const std::vector<Runtime::TupleBuffer>& StatisticOperatorHandler::getAllStatistics() const { return allStatistics; }
//
//const std::string& StatisticOperatorHandler::getLogicalSourceName() const { return logicalSourceName; }
//
//const std::string& StatisticOperatorHandler::getFieldName() const { return fieldName; }
//
//uint64_t StatisticOperatorHandler::getDepth() const { return depth; }
//
//const SchemaPtr& StatisticOperatorHandler::getSchema() const { return schema; }
//
//const Runtime::Execution::Operators::MultiOriginWatermarkProcessorPtr& StatisticOperatorHandler::getWatermarkProcessor() const {
//    return watermarkProcessor;
//}
//
//const Runtime::BufferManagerPtr& StatisticOperatorHandler::getBufferManager() const { return bufferManager; }

void StatisticOperatorHandler::setBufferManager(const Runtime::BufferManagerPtr& bufferManager) {
    StatisticOperatorHandler::bufferManager = bufferManager;
}

}// namespace NES::Experimental::Statistics