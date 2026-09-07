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

#include <Align/AlignFullMatchBuildEmitPhysicalOperator.hpp>

#include <optional>
#include <utility>
#include <Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

AlignFullMatchBuildEmitPhysicalOperator::AlignFullMatchBuildEmitPhysicalOperator(OperatorHandlerId operatorHandlerId, AlignBuildSide side)
    : operatorHandlerId(operatorHandlerId), side(side)
{
}

void AlignFullMatchBuildEmitPhysicalOperator::execute(ExecutionContext&, Record&) const
{
    throw NotImplemented("ALIGN() physical execution is not implemented yet; see RESEARCH_NOTES.md");
}

std::optional<PhysicalOperator> AlignFullMatchBuildEmitPhysicalOperator::getChild() const
{
    return child;
}

void AlignFullMatchBuildEmitPhysicalOperator::setChild(PhysicalOperator childOperator)
{
    this->child = std::move(childOperator);
}

}
