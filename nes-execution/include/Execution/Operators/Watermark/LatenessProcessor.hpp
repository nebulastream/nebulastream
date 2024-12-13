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

#pragma once

#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Execution/Operators/Watermark/MultiOriginWatermarkProcessor.hpp>


namespace NES::Runtime::Execution::Operators {

class LatenessProcessorOperatorHandler : public OperatorHandler
{
public:
    /// Updates the watermark processor with the new watermark info in bufferMetaData
    void updateSeenWatermarks(const BufferMetaData& bufferMetaData) const;

    Timestamp getCurrentWatermarkTimestamp() const;

private:
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
};

class LatenessProcessor : public ExecutableOperator {
public:
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    const uint64_t operatorHandlerIndex;
    std::unique_ptr<TimeFunction> timeFunction;
    Timestamp allowedLateness;
};

}
