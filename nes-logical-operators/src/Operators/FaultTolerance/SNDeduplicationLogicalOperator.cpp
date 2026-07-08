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
#include <Schema/Binder.hpp>
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

SNDeduplicationLogicalOperator::SNDeduplicationLogicalOperator(WeakLogicalOperator self, LogicalOperator child, std::string filePath)
    : ManagedByOperator(std::move(self)), child(std::move(child)), filePath(std::move(filePath))
{
    inferLocalSchema();
}

TypedLogicalOperator<SNDeduplicationLogicalOperator> SNDeduplicationLogicalOperator::create(std::string filePath)
{
    return TypedLogicalOperator<SNDeduplicationLogicalOperator>{std::move(filePath)};
}

TypedLogicalOperator<SNDeduplicationLogicalOperator> SNDeduplicationLogicalOperator::create(LogicalOperator child, std::string filePath)
{
    return TypedLogicalOperator<SNDeduplicationLogicalOperator>{std::move(child), std::move(filePath)};
}

std::string_view SNDeduplicationLogicalOperator::getName() const noexcept
{
    return NAME;
}

std::string SNDeduplicationLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("SN_DEDUPLICATION(opId: {}, traitSet: {}, filePath: {})", id, traitSet.explain(verbosity), filePath);
    }
    return "SN_DEDUPLICATION";
}

bool SNDeduplicationLogicalOperator::operator==(const SNDeduplicationLogicalOperator& rhs) const
{
    return filePath == rhs.filePath && outputSchema == rhs.outputSchema && traitSet == rhs.traitSet;
}

void SNDeduplicationLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();
    outputSchema = unbind(inputSchema);
}

SNDeduplicationLogicalOperator SNDeduplicationLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
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

SNDeduplicationLogicalOperator SNDeduplicationLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for selection, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

SNDeduplicationLogicalOperator SNDeduplicationLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for selection, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> SNDeduplicationLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> SNDeduplicationLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator SNDeduplicationLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

std::string SNDeduplicationLogicalOperator::getFilePath() const
{
    return filePath;
}

Reflected Reflector<TypedLogicalOperator<SNDeduplicationLogicalOperator>>::operator()(
    const TypedLogicalOperator<SNDeduplicationLogicalOperator>& op) const
{
    return reflect(detail::ReflectedSNDeduplicationLogicalOperator{.operatorId = op.getId(), .filePath = op->getFilePath()});
}

Unreflector<TypedLogicalOperator<SNDeduplicationLogicalOperator>>::Unreflector(ContextType operatorMapping)
    : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<SNDeduplicationLogicalOperator> Unreflector<TypedLogicalOperator<SNDeduplicationLogicalOperator>>::operator()(
    const Reflected& reflected, const ReflectionContext& context) const
{
    auto [id, filePath] = context.unreflect<detail::ReflectedSNDeduplicationLogicalOperator>(reflected);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("SelectionLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return SNDeduplicationLogicalOperator::create(children.at(0), filePath);
}


}

uint64_t std::hash<NES::SNDeduplicationLogicalOperator>::operator()(const NES::SNDeduplicationLogicalOperator& op) const noexcept
{
    return std::hash<std::string>{}(op.getFilePath()); //TODO check this
}