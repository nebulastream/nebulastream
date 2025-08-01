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

#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>

#include <algorithm>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

IngestionTimeWatermarkAssignerLogicalOperator::IngestionTimeWatermarkAssignerLogicalOperator() = default;

std::string_view IngestionTimeWatermarkAssignerLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string IngestionTimeWatermarkAssignerLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("INGESTIONTIMEWATERMARKASSIGNER(opId: {})", id);
    }
    return "INGESTION_TIME_WATERMARK_ASSIGNER";
}

LogicalOperator IngestionTimeWatermarkAssignerLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    PRECONDITION(inputSchemas.size() == 1, "Watermark assigner should have only one input");
    const auto& inputSchema = inputSchemas[0];
    copy.inputSchemas = inputSchemas;
    copy.outputSchema = inputSchema;
    return copy;
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterIngestionTimeWatermarkAssignerLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    return IngestionTimeWatermarkAssignerLogicalOperator();
}

}
