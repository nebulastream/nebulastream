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
#include <string>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Watermarks/IngestionTimeWatermarkStrategyDescriptor.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkStrategyDescriptor.hpp>
#include <Util/Common.hpp>

namespace NES::Windowing
{

IngestionTimeWatermarkStrategyDescriptor::IngestionTimeWatermarkStrategyDescriptor() = default;

std::shared_ptr<WatermarkStrategyDescriptor> IngestionTimeWatermarkStrategyDescriptor::create()
{
    return std::make_shared<IngestionTimeWatermarkStrategyDescriptor>(Windowing::IngestionTimeWatermarkStrategyDescriptor());
}

bool IngestionTimeWatermarkStrategyDescriptor::equal(std::shared_ptr<WatermarkStrategyDescriptor> other)
{
    return NES::Util::instanceOf<IngestionTimeWatermarkStrategyDescriptor>(other);
}

std::string IngestionTimeWatermarkStrategyDescriptor::toString()
{
    return "TYPE = INGESTION-TIME";
}

bool IngestionTimeWatermarkStrategyDescriptor::inferStamp(const std::shared_ptr<Schema>&)
{
    return true;
}

}
