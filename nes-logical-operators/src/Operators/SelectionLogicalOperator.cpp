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

#include <Operators/SelectionLogicalOperator.hpp>

#include <algorithm>
#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

SelectionLogicalOperator::SelectionLogicalOperator(LogicalFunction predicate) : data(std::move(predicate))
{
}

std::string_view SelectionLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalFunction SelectionLogicalOperator::getPredicate() const
{
    return data.predicate.value();
}


std::string SelectionLogicalOperator::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SELECTION(opId: {}, predicate: {})", id, data.predicate.value().explain(verbosity));
    }
    return fmt::format("SELECTION({})", data.predicate.value().explain(verbosity));
}

LogicalOperator SelectionLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    INVARIANT(!inputSchemas.empty(), "Selection should have at least one input");

    const auto& firstSchema = inputSchemas[0];
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Selection operator");
        }
    }

    copy.data.predicate = data.predicate.value().withInferredDataType(firstSchema);
    if (not copy.data.predicate.value().getDataType().isType(DataType::Type::BOOLEAN))
    {
        throw CannotInferSchema("the selection expression is not a valid predicate");
    }
    copy.inputSchemas = inputSchemas;
    copy.outputSchema = firstSchema;
    return copy;
}


LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSelectionLogicalOperator(NES::LogicalOperatorRegistryArguments)
{
    return SelectionLogicalOperator();
}
}
