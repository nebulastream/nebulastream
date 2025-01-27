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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES::Runtime::Execution::Util
{
std::shared_ptr<Schema> createJoinSchema(const std::shared_ptr<Schema>& leftSchema, const std::shared_ptr<Schema>& rightSchema)
{
    PRECONDITION(
        leftSchema->getLayoutType() == rightSchema->getLayoutType(),
        "Left and right schema do not have the same layout type (left: {} and right: {})",
        magic_enum::enum_name(leftSchema->getLayoutType()),
        magic_enum::enum_name(rightSchema->getLayoutType()));
    auto retSchema = Schema::create(leftSchema->getLayoutType());
    auto newQualifierForSystemField = leftSchema->getSourceNameQualifier() + rightSchema->getSourceNameQualifier();

    retSchema->addField(newQualifierForSystemField + "$start", BasicType::UINT64);
    retSchema->addField(newQualifierForSystemField + "$end", BasicType::UINT64);

    for (const auto& fields : *leftSchema)
    {
        retSchema->addField(fields->getName(), fields->getDataType());
    }

    for (const auto& fields : *rightSchema)
    {
        retSchema->addField(fields->getName(), fields->getDataType());
    }

    return retSchema;
}
}
