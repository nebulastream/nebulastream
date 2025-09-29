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

#include <../private/RawInputFormatScanWrapper.hpp>

#include <ostream>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>
#include <RawTupleBuffer.hpp>

namespace NES
{
void RawInputFormatScanWrapper::scan(
    ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer, const PhysicalOperator& child) const
{
    this->rawInputFormatScan->scanTask(executionCtx, recordBuffer, child);
}

void RawInputFormatScanWrapper::stop(PipelineExecutionContext&) const
{
    this->rawInputFormatScan->stopTask();
}

std::ostream& RawInputFormatScanWrapper::toString(std::ostream& os) const
{
    return this->rawInputFormatScan->toString(os);
}

}
