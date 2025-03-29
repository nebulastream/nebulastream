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
#include <utility>
#include <Nautilus/Interface/Record.hpp>
#include <MapPhysicalOperator.hpp>

namespace NES
{
MapPhysicalOperator::MapPhysicalOperator(std::vector<std::shared_ptr<TupleBufferMemoryProvider>> memoryProvider, Record::RecordFieldIdentifier fieldToWriteTo, std::unique_ptr<Functions::PhysicalFunction> mapFunction)
    : PhysicalOperator(std::move(memoryProvider)), fieldToWriteTo(std::move(fieldToWriteTo)), mapFunction(std::move(mapFunction))
{
}

void MapPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// execute map function
    const auto value = mapFunction->execute(record, ctx.pipelineMemoryProvider.arena);
    /// write the result to the record
    record.write(fieldToWriteTo, value);
    /// call next operator
    child()->execute(ctx, record);
}

}
