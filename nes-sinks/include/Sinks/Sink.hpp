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

#include <ostream>
#include <Sequencing/Sequencer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <fmt/ostream.h>
#include <BackpressureChannel.hpp>
#include <ExecutablePipelineStage.hpp>

namespace NES
{

class Sink : public ExecutablePipelineStage
{
public:
    enum class BufferResult : uint8_t
    {
        COMPLETED,
        RETRY,
    };

    explicit Sink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    ~Sink() override = default;
    friend std::ostream& operator<<(std::ostream& out, const Sink& sink);

    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) final;

    BackpressureController backpressureController;

protected:
    virtual BufferResult executeBuffer(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) = 0;

private:
    OutputOrderPolicy outputOrderPolicy;
    Sequencer<TupleBuffer> sequencer;
    DropOutOfOrderSequencer<TupleBuffer> dropOutOfOrderSequencer;
};

}

namespace fmt
{
template <>
struct formatter<NES::Sink> : ostream_formatter
{
};
}
