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

#pragma once

#include <memory>
#include <API/Schema.hpp>

namespace NES
{


/// This stores the left, right and output schema for a binary join
struct JoinSchema
{
    JoinSchema(
        const std::shared_ptr<Schema>& leftSchema, const std::shared_ptr<Schema>& rightSchema, const std::shared_ptr<Schema>& joinSchema)
        : leftSchema(leftSchema), rightSchema(rightSchema), joinSchema(joinSchema)
    {
    }

    std::shared_ptr<Schema> leftSchema;
    std::shared_ptr<Schema> rightSchema;
    std::shared_ptr<Schema> joinSchema;
};

namespace Util
{
/// Creates the join schema from the left and right schema
std::shared_ptr<Schema> createJoinSchema(const std::shared_ptr<Schema>& leftSchema, const std::shared_ptr<Schema>& rightSchema);
}
}
