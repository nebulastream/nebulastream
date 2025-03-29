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

#include <vector>
#include <Plans/Operator.hpp>

namespace NES
{

/// An operator, which creates an initializes a new origin id.
/// This are usually operators that create and emit new data records,
/// e.g., a Source, Window aggregation, or Join Operator.
/// Operators that only modify or select an already existing record, e.g.,
/// Filter or Map, dont need to assign new origin ids.
class OriginIdAssignmentOperator : public virtual Operator
{
public:
    OriginIdAssignmentOperator(OriginId originId = INVALID_ORIGIN_ID);

    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const;

    virtual void setOriginId(OriginId originId);
    [[nodiscard]] OriginId getOriginId() const;

protected:
    OriginId originId;
};

}
