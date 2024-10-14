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

#include <span>
#include <TestUtils/MockedPipelineExecutionContext.hpp>
namespace NES::Runtime::Execution
{

Memory::TupleBuffer copyBuffer(Memory::TupleBuffer& buffer, Memory::AbstractBufferProvider& provider)
{
    auto copiedBuffer = provider.getBufferBlocking();
    NES_ASSERT(copiedBuffer.getBufferSize() >= buffer.getBufferSize(), "Attempt to copy buffer into smaller buffer");
    auto bufferData = std::span(buffer.getBuffer(), buffer.getBufferSize());
    std::ranges::copy(bufferData, copiedBuffer.getBuffer());
    copiedBuffer.setWatermark(buffer.getWatermark());
    copiedBuffer.setChunkNumber(buffer.getChunkNumber());
    copiedBuffer.setSequenceNumber(buffer.getSequenceNumber());
    copiedBuffer.setCreationTimestampInMS(buffer.getCreationTimestampInMS());
    copiedBuffer.setLastChunk(buffer.isLastChunk());
    copiedBuffer.setOriginId(buffer.getOriginId());
    copiedBuffer.setSequenceData(buffer.getSequenceData());
    copiedBuffer.setNumberOfTuples(buffer.getNumberOfTuples());

    for (size_t childIdx = 0; childIdx < buffer.getNumberOfChildrenBuffer(); ++childIdx)
    {
        auto childBuffer = buffer.loadChildBuffer(childIdx);
        auto copiedChildBuffer = copyBuffer(childBuffer, provider);
        NES_ASSERT(copiedBuffer.storeChildBuffer(copiedChildBuffer) == childIdx, "Child buffer index does not match");
    }

    return copiedBuffer;
}

MockedPipelineExecutionContext::MockedPipelineExecutionContext(
    std::vector<OperatorHandlerPtr> handler,
    bool logSeenSeqChunk,
    std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
    : PipelineExecutionContext(
          INVALID_PIPELINE_ID, /// mock pipeline id
          INVALID_QUERY_ID, /// mock query id
          std::move(bufferProvider),
          1,
          [this, logSeenSeqChunk](Memory::TupleBuffer& buffer, Runtime::WorkerContextRef)
          {
              if (logSeenSeqChunk)
              {
                  SequenceData seqChunkLastChunk = {buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()};
                  const auto it = std::find(seenSeqChunkLastChunk.begin(), seenSeqChunkLastChunk.end(), seqChunkLastChunk);
                  if (it != seenSeqChunkLastChunk.end())
                  {
                      NES_ERROR("Already seen triplet of {}", seqChunkLastChunk.toString());
                  }
                  seenSeqChunkLastChunk.insert(seqChunkLastChunk);
              }

              buffers.emplace_back(copyBuffer(buffer, *this->getBufferManager()));
          },
          [this, logSeenSeqChunk](Memory::TupleBuffer& buffer)
          {
              if (logSeenSeqChunk)
              {
                  SequenceData seqChunkLastChunk = {buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()};
                  const auto it = std::find(seenSeqChunkLastChunk.begin(), seenSeqChunkLastChunk.end(), seqChunkLastChunk);
                  if (it != seenSeqChunkLastChunk.end())
                  {
                      NES_ERROR("Already seen triplet of {}", seqChunkLastChunk.toString());
                  }
                  seenSeqChunkLastChunk.insert(seqChunkLastChunk);
              }

              buffers.emplace_back(copyBuffer(buffer, *this->getBufferManager()));
          },
          std::move(handler)) {
        /// nop
    };

} /// namespace NES::Runtime::Execution
