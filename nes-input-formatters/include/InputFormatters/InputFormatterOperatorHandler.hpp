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

struct StagedTupleBuffer
{
    NES::Memory::TupleBuffer tupleBuffer;
    uint64_t offset = 0;

    [[nodiscard]] std::string_view getStringView() const
    {
        return {tupleBuffer.getBuffer<const char>() + offset, tupleBuffer.getBufferSize() - offset};
    }
};

class TupleBufferStagingArea
{
public:
    TupleBufferStagingArea(const size_t sizeOfStagingArea) : activeBuffers(0)
    {
        buffers.reserve(sizeOfStagingArea);
    }
    std::optional<StagedTupleBuffer> getPriorStagedTupleBuffer(const size_t stagedTupleBufferIndex)
    {
        return buffers.at(stagedTupleBufferIndex - 1);
    }
    std::optional<StagedTupleBuffer> getNextStagedTupleBuffer(const size_t stagedTupleBufferIndex)
    {
        if (stagedTupleBufferIndex + 1 >= buffers.size())
        {
            return std::nullopt;
        }
        return buffers.at(stagedTupleBufferIndex + 1);
    }

    const StagedTupleBuffer& getTupleBuffer(const size_t index)
    {
        PRECONDITION(buffers.size() > index, fmt::format("Accessing staged TupleBuffer with size {} at index {}", buffers.size(), index));
        return buffers[index];
    }

    void setTupleBuffer(NES::Memory::TupleBuffer tupleBuffer, const uint64_t offset, const size_t index)
    {
        PRECONDITION(buffers.size() > index, fmt::format("Accessing staged TupleBuffer with size {} at index {}", buffers.size(), index));
        buffers[index] = StagedTupleBuffer{.tupleBuffer = std::move(tupleBuffer), .offset = offset};
    }

    void pushTupleBuffer(NES::Memory::TupleBuffer tupleBuffer, const uint64_t offset)
    {
        buffers.emplace_back(StagedTupleBuffer{.tupleBuffer = std::move(tupleBuffer), .offset = offset});
        ++activeBuffers;
    }

    // Todo: implement properly
    bool hasPartialTuple() const
    {
        return activeBuffers > 0;
    }

private:
    std::vector<StagedTupleBuffer> buffers;
    size_t activeBuffers;
};

class InputFormatterOperatorHandler : public NES::Runtime::Execution::OperatorHandler
{
public:
    InputFormatterOperatorHandler() = default;

    /// pipelineExecutionContext, localStateVariableId
    // pipelineExecutionContext
    void start(NES::Runtime::Execution::PipelineExecutionContext&, uint32_t) override
    {
        /*noop*/
    }
    /// terminationType, pipelineExecutionContext
    void stop(
        NES::Runtime::QueryTerminationType, NES::Runtime::Execution::PipelineExecutionContext&) override
    {
        /*noop*/
        /// Todo: destroy staging area
    }

    TupleBufferStagingArea& getTupleBufferStagingArea()
    {
        return tbStagingArea;
    }

    bool hasPartialTuple() const
    {
        return tbStagingArea.hasPartialTuple();
    }

private:
    // Todo: get rid of hardcoded 10
    TupleBufferStagingArea tbStagingArea{10};
};