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

#include <optional>
#include <utility>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalOperator.hpp>
#include <UnionPhysicalOperator.hpp>

namespace NES
{

void UnionPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Path-through, will be optimized out during query compilation
    executeChild(ctx, record);
}

std::optional<PhysicalOperator> UnionPhysicalOperator::getChild() const
{
    return child;
}

void UnionPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
