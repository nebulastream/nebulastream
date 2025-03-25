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
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime::Execution::Util
{
Schema createJoinSchema(Schema leftSchema, Schema rightSchema)
{
    PRECONDITION(
        leftSchema.memoryLayoutType == rightSchema.memoryLayoutType,
        "Left and right schema do not have the same layout type (left: {} and right: {})",
        magic_enum::enum_name(leftSchema.memoryLayoutType),
        magic_enum::enum_name(rightSchema.memoryLayoutType));
    auto retSchema = Schema{leftSchema.memoryLayoutType};
    auto newQualifierForSystemField = leftSchema.getSourceNameQualifier() + rightSchema.getSourceNameQualifier();

    retSchema.addField(newQualifierForSystemField + Identifier{"start"}, DataType::Type::UINT64);
    retSchema.addField(newQualifierForSystemField + Identifier{"end"}, DataType::Type::UINT64);

    for (const auto& fields : leftSchema.getFields())
    {
        retSchema.addField(fields.name, fields.dataType);
    }

    for (const auto& fields : rightSchema.getFields())
    {
        retSchema.addField(fields.name, fields.dataType);
    }

    return retSchema;
}
}
