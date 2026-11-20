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

#include <MapPhysicalOperator.hpp>

#include <optional>
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{
MapPhysicalOperator::MapPhysicalOperator(Record::RecordFieldIdentifier fieldToWriteTo, PhysicalFunction mapFunction)
    : assignments{{std::move(fieldToWriteTo), std::move(mapFunction)}}
{
}

MapPhysicalOperator::MapPhysicalOperator(std::vector<std::pair<Record::RecordFieldIdentifier, PhysicalFunction>> assignments)
    : assignments(std::move(assignments))
{
}

void MapPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// evaluate every function on the record as it arrived, then write the results
    std::vector<VarVal> values;
    values.reserve(assignments.size());
    for (const auto& [field, function] : assignments)
    {
        values.push_back(function.execute(record, ctx.pipelineMemoryProvider.arena));
    }
    for (size_t i = 0; i < assignments.size(); ++i)
    {
        record.write(assignments[i].first, values[i]);
    }
    /// call next operator
    executeChild(ctx, record);
}

std::optional<PhysicalOperator> MapPhysicalOperator::getChild() const
{
    return child;
}

void MapPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
