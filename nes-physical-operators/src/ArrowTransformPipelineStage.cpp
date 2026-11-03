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

#include <ArrowTransformPipelineStage.hpp>

#include <cstddef>
#include <memory>
#include <utility>

#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <Interface/BufferRef/ArrowCData.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
template <typename Node>
struct ReleaseGuard
{
    explicit ReleaseGuard(Node& node) : node(node) { }

    ReleaseGuard(const ReleaseGuard&) = delete;
    ReleaseGuard& operator=(const ReleaseGuard&) = delete;

    ~ReleaseGuard()
    {
        if (node.release != nullptr)
        {
            node.release(&node);
        }
    }

private:
    Node& node; /// NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members) the guard never outlives the node
};
}

ArrowTransformPipelineStage::ArrowTransformPipelineStage(
    std::shared_ptr<ArrowBufferRef> inputLayout, std::shared_ptr<ArrowBufferRef> outputLayout, Transform transform)
    : inputLayout(std::move(inputLayout)), outputLayout(std::move(outputLayout)), transform(std::move(transform))
{
}

void ArrowTransformPipelineStage::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    ArrowArray input{};
    ArrowSchema inputSchema{};
    ArrowArray output{};
    ArrowSchema outputSchema{};
    const ReleaseGuard inputGuard{input};
    const ReleaseGuard inputSchemaGuard{inputSchema};
    const ReleaseGuard outputGuard{output};
    const ReleaseGuard outputSchemaGuard{outputSchema};

    Arrow::exportBatch(inputTupleBuffer, *inputLayout, input, inputSchema);
    transform(input, inputSchema, output, outputSchema);
    auto outputBuffers = Arrow::importBatch(output, outputSchema, *outputLayout, *pipelineExecutionContext.getBufferManager());

    for (size_t i = 0; i < outputBuffers.size(); ++i)
    {
        auto& buffer = outputBuffers[i];
        buffer.setOriginId(inputTupleBuffer.getOriginId());
        buffer.setSequenceNumber(inputTupleBuffer.getSequenceNumber());
        buffer.setWatermark(inputTupleBuffer.getWatermark());
        buffer.setCreationTimestampInMS(inputTupleBuffer.getCreationTimestampInMS());
        chunkNumbers.setChunkNumber(i + 1 == outputBuffers.size(), inputTupleBuffer.getChunkNumber(), inputTupleBuffer.isLastChunk(), buffer);
        pipelineExecutionContext.emitBuffer(buffer);
    }
}

}
