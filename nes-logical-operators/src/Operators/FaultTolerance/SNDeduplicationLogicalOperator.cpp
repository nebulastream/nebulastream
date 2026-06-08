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

#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <Operators/FaultTolerance/SNDeduplicationLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{
SNDeduplicationLogicalOperator::SNDeduplicationLogicalOperator(WeakLogicalOperator self, std::string filePath)
    : ManagedByOperator(std::move(self)), filePath(std::move(filePath))
{
}

std::string_view SNDeduplicationLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string SNDeduplicationLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "SN_DEDUPLICATION(opId: {}, inputSchema: {}, traitSet: {}, filePath: {})",
            id,
            inputSchema,
            traitSet.explain(verbosity),
            filePath);
    }
    return "SN_DEDUPLICATION";
}

bool SNDeduplicationLogicalOperator::operator==(const SNDeduplicationLogicalOperator& rhs) const
{
    return getOutputSchema() == rhs.getOutputSchema() && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
}

SNDeduplicationLogicalOperator SNDeduplicationLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (inputSchemas.empty())
    {
        throw CannotDeserialize("SNDeduplication should have at least one input");
    }

    const auto& firstSchema = inputSchemas.at(0);
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for SNDeduplication operator");
        }
    }
    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;
    return copy;
}

TraitSet SNDeduplicationLogicalOperator::getTraitSet() const
{
    return traitSet;
}

SNDeduplicationLogicalOperator SNDeduplicationLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

SNDeduplicationLogicalOperator SNDeduplicationLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> SNDeduplicationLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema SNDeduplicationLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> SNDeduplicationLogicalOperator::getChildren() const
{
    return children;
}

std::string SNDeduplicationLogicalOperator::getFilePath() const
{
    return filePath;
}


Reflected Reflector<TypedLogicalOperator<SNDeduplicationLogicalOperator>>::operator()(
    const TypedLogicalOperator<SNDeduplicationLogicalOperator>& op) const
{
    return reflect(op->filePath);
}

TypedLogicalOperator<SNDeduplicationLogicalOperator>
Unreflector<TypedLogicalOperator<SNDeduplicationLogicalOperator>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto filePath = context.unreflect<std::string>(reflected);
    return TypedLogicalOperator<SNDeduplicationLogicalOperator>{filePath};
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterSNDeduplicationLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return ReflectionContext{}.unreflect<TypedLogicalOperator<SNDeduplicationLogicalOperator>>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only build directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
