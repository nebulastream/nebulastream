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

#include <cstdint>
#include <optional>
#include <vector>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
// #include <InputFormatters/SequenceShredder.hpp>
// #include <InputFormatters/CircularBuffer.hpp>

namespace NES::InputFormatters::InputFormatterProvider
{

// Todo: make ring buffer
// - is synchronization necessary?
// - when to place buffer?
//  -> current implementation would hint add a double check
//  -> check ring buffer first, then SequenceShredder
//      -> sequence shredder actually contains ring buffer?
//      -> sequence shredder could return vector of buffers
//          -> move buffer refs to vector, then overwrite slots
struct StagedTupleBuffer
{
    std::optional<NES::Memory::TupleBuffer> tupleBuffer;
    uint64_t offset = 0;

    [[nodiscard]] std::string_view getStringView() const
    {
        if(tupleBuffer.has_value())
        {
            return {tupleBuffer.value().getBuffer<const char>() + offset, tupleBuffer.value().getBufferSize() - offset};
        }
        return "";
    }
};

class InputFormatterOperatorHandler : public NES::Runtime::Execution::OperatorHandler
{
public:
    InputFormatterOperatorHandler() = default;

    /// pipelineExecutionContext, localStateVariableId
    // pipelineExecutionContext
    void start(NES::Runtime::Execution::PipelineExecutionContext&, uint32_t) override { /*noop*/ }
    /// terminationType, pipelineExecutionContext
    void stop(NES::Runtime::QueryTerminationType, NES::Runtime::Execution::PipelineExecutionContext&) override
    {
        /*noop*/
        /// Todo: destroy staging area
    }

private:
    // Todo: get rid of hardcoded 10
    // SequenceShredder sequenceShredder;
    std::vector<StagedTupleBuffer> stagedTupleBuffers;
};

}