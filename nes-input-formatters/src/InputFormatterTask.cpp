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

#include <memory>
#include <ostream>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <PipelineExecutionContext.hpp>
#include <SequenceShredder.hpp>

namespace NES::InputFormatters
{

InputFormatterTask::InputFormatterTask(const OriginId originId, std::unique_ptr<InputFormatter> inputFormatter)
    : originId(originId)
    , sequenceShredder(std::make_unique<SequenceShredder>(inputFormatter->getSizeOfTupleDelimiter()))
    , inputFormatter(std::move(inputFormatter))
{
}
InputFormatterTask::~InputFormatterTask() = default;
void InputFormatterTask::stop(PipelineExecutionContext& pipelineExecutionContext)
{
    inputFormatter->flushFinalTuple(originId, pipelineExecutionContext, *sequenceShredder);
}

void InputFormatterTask::execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext)
{
    pipelineExecutionContext.formattingTask = true;
    inputFormatter->parseTupleBufferRaw(
        inputTupleBuffer, pipelineExecutionContext, inputTupleBuffer.getUsedMemorySize(), *sequenceShredder);
}
std::ostream& InputFormatterTask::toString(std::ostream& os) const
{
    os << "InputFormatterTask: {\n";
    os << "  originId: " << originId << ",\n";
    os << *inputFormatter;
    os << *sequenceShredder;
    os << "}\n";
    return os;
}
}
