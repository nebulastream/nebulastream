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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Util/Logger/Logger.hpp>

#include <fstream>

namespace NES::Runtime::Execution::Util {

SchemaPtr createJoinSchema(const SchemaPtr& leftSchema, const SchemaPtr& rightSchema, const std::string& keyFieldName) {
    NES_ASSERT(leftSchema->getLayoutType() == rightSchema->getLayoutType(),
               "Left and right schema do not have the same layout type");
    NES_ASSERT(leftSchema->contains(keyFieldName) || rightSchema->contains(keyFieldName) || leftSchema->hasFieldName(keyFieldName)
                   || rightSchema->hasFieldName(keyFieldName),
               "KeyFieldName = " << keyFieldName << " is not in either left or right schema");

    auto retSchema = Schema::create(leftSchema->getLayoutType());
    auto newQualifierForSystemField = leftSchema->getSourceNameQualifier() + rightSchema->getSourceNameQualifier();

    retSchema->addField(newQualifierForSystemField + "$start", BasicType::UINT64);
    retSchema->addField(newQualifierForSystemField + "$end", BasicType::UINT64);
    retSchema->addField(newQualifierForSystemField + "$key", BasicType::UINT64);

    for (auto& fields : leftSchema->fields) {
        retSchema->addField(fields->getName(), fields->getDataType());
    }

    for (auto& fields : rightSchema->fields) {
        retSchema->addField(fields->getName(), fields->getDataType());
    }
    NES_DEBUG("Created joinSchema {} from leftSchema {} and rightSchema {}.",
              retSchema->toString(),
              leftSchema->toString(),
              rightSchema->toString());

    return retSchema;
}

}// namespace NES::Runtime::Execution::Util