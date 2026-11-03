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

#include <functional>
#include <memory>
#include <ostream>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Interface/BufferRef/ArrowCData.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <EmitOperatorHandler.hpp>
#include <ExecutablePipelineStage.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// Hands every input buffer as an Arrow record batch to a user-defined transformation and emits the batch it returns.
/// It sits between two compiled pipelines: the predecessor ends in an EmitPhysicalOperator and the successor starts with a
/// ScanPhysicalOperator, both using an ArrowBufferRef. The input is passed without copying, the output is copied into
/// TupleBuffers.
class ArrowTransformPipelineStage final : public ExecutablePipelineStage
{
public:
    /// Receives ownership of the input batch and has to fill the output batch with a release callback. If the callback does
    /// not move the input, the stage releases it. Throwing fails the query.
    using Transform = std::function<void(ArrowArray& input, ArrowSchema& inputSchema, ArrowArray& output, ArrowSchema& outputSchema)>;

    ArrowTransformPipelineStage(std::shared_ptr<ArrowBufferRef> inputLayout, std::shared_ptr<ArrowBufferRef> outputLayout, Transform transform);

    void start(PipelineExecutionContext&) override { }

    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;

    void stop(PipelineExecutionContext&) override { }

protected:
    std::ostream& toString(std::ostream& os) const override { return os << "ArrowTransformPipelineStage"; }

private:
    std::shared_ptr<ArrowBufferRef> inputLayout;
    std::shared_ptr<ArrowBufferRef> outputLayout;
    Transform transform;
    /// A batch can grow and thus span more buffers than it came in, which requires new chunk numbers.
    EmitOperatorHandler chunkNumbers;
};

}
