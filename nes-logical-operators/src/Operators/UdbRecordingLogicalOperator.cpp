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

#include <Operators/UdbRecordingLogicalOperator.hpp>

#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>

namespace NES
{

std::string UdbRecordingLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId id) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format("UDB_RECORDING(opId: {}, traceName: {})", id, traceName.value_or("<auto>"));
    }
    return {"UDB_RECORDING"};
}

std::string_view UdbRecordingLogicalOperator::getName() noexcept
{
    return NAME;
}

UdbRecordingLogicalOperator UdbRecordingLogicalOperator::withTraitSet(TraitSet ts) const
{
    auto copy = *this;
    copy.traitSet = std::move(ts);
    return copy;
}

TraitSet UdbRecordingLogicalOperator::getTraitSet() const
{
    return traitSet;
}

UdbRecordingLogicalOperator UdbRecordingLogicalOperator::withChildren(std::vector<LogicalOperator> newChildren) const
{
    auto copy = *this;
    copy.children = std::move(newChildren);
    return copy;
}

std::vector<LogicalOperator> UdbRecordingLogicalOperator::getChildren() const
{
    return children;
}

std::vector<Schema> UdbRecordingLogicalOperator::getInputSchemas() const
{
    INVARIANT(!children.empty(), "UdbRecording operator requires exactly one child");
    return children | std::ranges::views::transform([](const LogicalOperator& child) { return child.getOutputSchema(); })
        | std::ranges::to<std::vector>();
}

Schema UdbRecordingLogicalOperator::getOutputSchema() const
{
    INVARIANT(!children.empty(), "UdbRecording operator requires exactly one child");
    return children.front().getOutputSchema();
}

UdbRecordingLogicalOperator UdbRecordingLogicalOperator::withInferredSchema(std::vector<Schema>) const
{
    auto copy = *this;
    return copy;
}

bool UdbRecordingLogicalOperator::operator==(const UdbRecordingLogicalOperator& rhs) const
{
    return getOutputSchema() == rhs.getOutputSchema() && getTraitSet() == rhs.getTraitSet() && traceName == rhs.traceName;
}

}

namespace NES
{

Reflected Reflector<UdbRecordingLogicalOperator>::operator()(const UdbRecordingLogicalOperator& op) const
{
    return reflect(detail::ReflectedUdbRecordingLogicalOperator{.traceName = op.getTraceName()});
}

UdbRecordingLogicalOperator Unreflector<UdbRecordingLogicalOperator>::operator()(const Reflected& reflected) const
{
    auto [traceName] = unreflect<detail::ReflectedUdbRecordingLogicalOperator>(reflected);
    return UdbRecordingLogicalOperator(std::move(traceName));
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterUdbRecordingLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    if (!arguments.reflected.isEmpty())
    {
        return unreflect<UdbRecordingLogicalOperator>(arguments.reflected);
    }
    PRECONDITION(false, "Operator is only built directly via parser or via reflection, not using the registry");
    std::unreachable();
}

}
