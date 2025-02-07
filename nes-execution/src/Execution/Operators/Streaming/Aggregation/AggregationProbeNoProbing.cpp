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

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationProbeNoProbing.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/WindowAggregationOperator.hpp>
#include <Execution/Operators/Streaming/WindowOperatorProbe.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Operators
{

void AggregationProbeNoProbing::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    Operator::open(executionCtx, recordBuffer);

    /// This operator does not do anything
}

AggregationProbeNoProbing::AggregationProbeNoProbing(
    WindowAggregationOperator windowAggregationOperator, const uint64_t operatorHandlerIndex, WindowMetaData windowMetaData)
    : WindowAggregationOperator(std::move(windowAggregationOperator)), WindowOperatorProbe(operatorHandlerIndex, std::move(windowMetaData))
{
}

}
