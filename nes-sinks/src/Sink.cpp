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

#include <Sinks/Sink.hpp>

#include <optional>
#include <ostream>
#include <utility>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <Sinks/SinkDescriptor.hpp>

namespace NES
{

namespace
{
SequenceData getSequenceData(const TupleBuffer& buffer)
{
    return {buffer.getSequenceNumber(), buffer.getChunkNumber(), buffer.isLastChunk()};
}

template <typename SequencerType, typename Process>
void processOrdered(SequencerType& sequencer, const TupleBuffer& inputBuffer, Process&& process)
{
    auto current = sequencer.take(getSequenceData(inputBuffer), inputBuffer);
    while (current)
    {
        const auto sequence = getSequenceData(*current);
        if (process(*current) == Sink::BufferResult::RETRY)
        {
            sequencer.defer(sequence);
            return;
        }
        current = sequencer.acknowledge(sequence);
    }
}
}

Sink::Sink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : backpressureController(std::move(backpressureController))
    , outputOrderPolicy(sinkDescriptor.getFromConfig(SinkDescriptor::OUTPUT_ORDER))
{
}

void Sink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    switch (outputOrderPolicy)
    {
        case OutputOrderPolicy::IGNORE_ORDER:
            executeBuffer(inputTupleBuffer, pipelineExecutionContext);
            return;
        case OutputOrderPolicy::ENFORCE_ORDER:
            processOrdered(
                sequencer,
                inputTupleBuffer,
                [this, &pipelineExecutionContext](const TupleBuffer& buffer)
                { return executeBuffer(buffer, pipelineExecutionContext); });
            return;
        case OutputOrderPolicy::DROP_OUT_OF_ORDER:
            processOrdered(
                dropOutOfOrderSequencer,
                inputTupleBuffer,
                [this, &pipelineExecutionContext](const TupleBuffer& buffer)
                { return executeBuffer(buffer, pipelineExecutionContext); });
            return;
    }
}

std::ostream& operator<<(std::ostream& out, const Sink& sink)
{
    return sink.toString(out);
}

}
