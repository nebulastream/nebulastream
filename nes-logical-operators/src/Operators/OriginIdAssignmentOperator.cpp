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

#include <Plans/Operator.hpp>
#include <Operators/OriginIdAssignmentOperator.hpp>
#include <ErrorHandling.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES
{

OriginIdAssignmentOperator::OriginIdAssignmentOperator(OriginId originId) : Operator(), originId(originId)
{
}

void OriginIdAssignmentOperator::setOriginId(OriginId originId)
{
    this->originId = originId;
}

std::vector<OriginId> OriginIdAssignmentOperator::getOutputOriginIds() const
{
    INVARIANT(
        originId != INVALID_ORIGIN_ID,
        "The origin id should not be invalid, but was {}. Maybe the OriginIdInference rule was not executed.",
        originId.getRawValue());
    return {originId};
}

OriginId OriginIdAssignmentOperator::getOriginId() const
{
    INVARIANT(
        originId != INVALID_ORIGIN_ID,
        "The origin id should not be invalid, but was {}. Maybe the OriginIdInference rule was not executed.",
        originId.getRawValue());
    return originId;
}

}
