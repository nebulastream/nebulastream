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

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::InputFormatters {


ExecutionResult InputFormatter::execute(
    Memory::TupleBuffer& inputTupleBuffer,
    Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
    Runtime::WorkerContext& workerContext)
{
    parseTupleBufferRaw(inputTupleBuffer, pipelineExecutionContext, workerContext, inputTupleBuffer.getNumberOfTuples());
    return ExecutionResult::Ok;
    /// Todo: could use pipelineExecutionContext.emitBuffer() instead of custom emit function.
}
// void InputFormatter::emitWork(Memory::TupleBuffer& buffer, const bool addBufferMetaData)
// {
//     if (addBufferMetaData)
//     {
//         /// set the origin id for this source
//         buffer.setOriginId(originId); //Todo: fix
//         /// set the creation timestamp
//         buffer.setCreationTimestampInMS(Timestamp(
//             std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
//         /// Set the sequence number of this buffer.
//         /// A data source generates a monotonic increasing sequence number
//         ++maxSequenceNumber;
//         buffer.setSequenceNumber(SequenceNumber(maxSequenceNumber));
//         buffer.setChunkNumber(ChunkNumber(1));
//         buffer.setLastChunk(true);
//         NES_DEBUG(
//             "Setting the buffer metadata for source {} with originId={} sequenceNumber={} chunkNumber={} lastChunk={}",
//             buffer.getOriginId(),
//             buffer.getOriginId(),
//             buffer.getSequenceNumber(),
//             buffer.getChunkNumber(),
//             buffer.isLastChunk());
//     }
//     emitFunction(originId, ReturnType::Data{buffer});
// }

}