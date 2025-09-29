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


#include <InputFormatters/RawScanPhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <Nautilus/Interface/RecordBuffer.hpp>
#include <PhysicalOperator.hpp>
#include <RawInputFormatScanWrapper.hpp>

namespace NES
{

RawScanPhysicalOperator::RawScanPhysicalOperator(std::unique_ptr<RawInputFormatScanWrapper> rawInputFormatScan)
    : rawInputFormatScan(std::move(rawInputFormatScan))
{
}

void RawScanPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    this->rawInputFormatScan->scan(executionCtx, recordBuffer, child.value());
}

std::optional<PhysicalOperator> RawScanPhysicalOperator::getChild() const
{
    return child;
}

void RawScanPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
