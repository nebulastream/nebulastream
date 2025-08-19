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

#include <InputFormatters/InputFormatterTaskPipeline.hpp>

#include <ostream>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawTupleBuffer.hpp>

void NES::InputFormatterTaskPipeline::start(PipelineExecutionContext&)
{
    this->inputFormatterTask->startTask();
}

void NES::InputFormatterTaskPipeline::stop(PipelineExecutionContext&)
{
    this->inputFormatterTask->stopTask();
}

void NES::InputFormatterTaskPipeline::execute(const TupleBuffer& rawTupleBuffer, PipelineExecutionContext& pec)
{
    /// If the buffer is empty, we simply return without submitting any unnecessary work on empty buffers.
    if (rawTupleBuffer.getBufferSize() == 0)
    {
        NES_WARNING("Received empty buffer in InputFormatterTask.");
        return;
    }

    this->inputFormatterTask->executeTask(RawTupleBuffer{rawTupleBuffer}, pec);
}

std::ostream& NES::InputFormatterTaskPipeline::toString(std::ostream& os) const
{
    return this->inputFormatterTask->toString(os);
}
