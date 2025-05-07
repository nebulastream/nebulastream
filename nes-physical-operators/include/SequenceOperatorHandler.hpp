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
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/Sequencer.hpp>

namespace NES::Runtime::Execution::Operators
{

class SequenceOperatorHandler final : public OperatorHandler
{
public:
    SequenceOperatorHandler() = default;

    std::optional<Memory::TupleBuffer*> getNextBuffer(uint8_t* buffer);
    std::optional<Memory::TupleBuffer*> markBufferAsDone(Memory::TupleBuffer* buffer);

    void start(PipelineExecutionContext&, uint32_t) override { sequencer = {}; }
    void stop(QueryTerminationType, PipelineExecutionContext&) override { /*noop*/ }

    Sequencer<Memory::TupleBuffer> sequencer;
    Memory::TupleBuffer currentBuffer;
};
}
