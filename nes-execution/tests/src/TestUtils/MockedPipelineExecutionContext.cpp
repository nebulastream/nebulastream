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

#include <TestUtils/MockedPipelineExecutionContext.hpp>
#include <utility>
namespace NES::Runtime::Execution {

MockedPipelineExecutionContext::MockedPipelineExecutionContext(std::vector<OperatorHandlerPtr> handler)
    : MockedPipelineExecutionContext(std::move(handler), true) {}

MockedPipelineExecutionContext::MockedPipelineExecutionContext() : MockedPipelineExecutionContext(true) {}

MockedPipelineExecutionContext::MockedPipelineExecutionContext(bool logSeenSeqChunk)
    : MockedPipelineExecutionContext(std::vector<OperatorHandlerPtr>(), logSeenSeqChunk) {}

MockedPipelineExecutionContext::MockedPipelineExecutionContext(std::vector<OperatorHandlerPtr> handler,
                                                               bool logSeenSeqChunk)
    : PipelineExecutionContext(
        -1,// mock pipeline id
        0, // mock query id
        nullptr,
        1,
        [this, logSeenSeqChunk](TupleBuffer& buffer, Runtime::WorkerContextRef) {
              if (logSeenSeqChunk) {
                  SequenceData seqChunkLastChunk = {buffer.getSequenceNumber(),
                                                         buffer.getChunkNumber(),
                                                         buffer.isLastChunk()};
                  const auto it = std::find(seenSeqChunkLastChunk.begin(), seenSeqChunkLastChunk.end(), seqChunkLastChunk);
                  if (it != seenSeqChunkLastChunk.end()) {
                      NES_ERROR("Already seen triplet of {}", seqChunkLastChunk.toString());
                  }
                  seenSeqChunkLastChunk.insert(seqChunkLastChunk);
              }
              buffers.emplace_back(std::move(buffer));
        },
        [this, logSeenSeqChunk](TupleBuffer& buffer) {
              if (logSeenSeqChunk) {
                  SequenceData seqChunkLastChunk = {buffer.getSequenceNumber(),
                                                         buffer.getChunkNumber(),
                                                         buffer.isLastChunk()};
                  const auto it = std::find(seenSeqChunkLastChunk.begin(), seenSeqChunkLastChunk.end(), seqChunkLastChunk);
                  if (it != seenSeqChunkLastChunk.end()) {
                      NES_ERROR("Already seen triplet of {}", seqChunkLastChunk.toString());
                  }
                  seenSeqChunkLastChunk.insert(seqChunkLastChunk);
              }
              buffers.emplace_back(std::move(buffer));
        },
        std::move(handler)){
        // nop
    };

}// namespace NES::Runtime::Execution