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

#include <Execution/Functions/Function.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Map.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators
{
Map::Map(Record::RecordFieldIdentifier fieldToWriteTo, std::unique_ptr<Functions::Function> mapFunction)
    : fieldToWriteTo(std::move(fieldToWriteTo)), mapFunction(std::move(mapFunction))
{
}

void Map::execute(ExecutionContext& ctx, Record& record) const
{
    /// execute map function
    const auto value = mapFunction->execute(record, ctx.pipelineMemoryProvider.arena);
    /// write the result to the record
    record.write(fieldToWriteTo, value);
    /// call next operator
    child->execute(ctx, record);
}

}
