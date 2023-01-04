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
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Runtime::Execution::Util {

uint64_t murmurHash(uint64_t key) {
    uint64_t hash = key;

    hash ^= hash >> 33;
    hash *= UINT64_C(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash *= UINT64_C(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;

    return hash;
}

SchemaPtr createJoinSchema(SchemaPtr leftSchema, SchemaPtr rightSchema, const std::string& keyFieldName) {
    NES_ASSERT(leftSchema->getLayoutType() == rightSchema->getLayoutType(),
               "Left and right schema do not have the same layout type");
    NES_ASSERT(leftSchema->contains(keyFieldName) || rightSchema->contains(keyFieldName),
               "KeyFieldName = " << keyFieldName << " is not in either left or right schema");

    auto retSchema = Schema::create(leftSchema->getLayoutType());
    if (leftSchema->contains(keyFieldName)) {
        retSchema->addField(leftSchema->get(keyFieldName));
    } else {
        retSchema->addField(rightSchema->get(keyFieldName));
    }

    retSchema->copyFields(leftSchema);
    retSchema->copyFields(rightSchema);

    return retSchema;
}
}// namespace NES::Runtime::Execution::Util