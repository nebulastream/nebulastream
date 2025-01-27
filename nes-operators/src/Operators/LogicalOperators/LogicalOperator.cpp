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

#include <utility>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/OperatorState.hpp>
#include <ErrorHandling.hpp>
#include <magic_enum.hpp>

namespace NES
{

LogicalOperator::LogicalOperator(OperatorId id) : Operator(id)
{
}

std::map<size_t, std::set<std::string>> LogicalOperator::getHashBasedSignature() const
{
    return hashBasedSignature;
}

void LogicalOperator::setHashBasedSignature(std::map<size_t, std::set<std::string>> signature)
{
    this->hashBasedSignature = std::move(signature);
}

void LogicalOperator::updateHashBasedSignature(size_t hashCode, const std::string& stringSignature)
{
    if (hashBasedSignature.contains(hashCode))
    {
        auto stringSignatures = hashBasedSignature[hashCode];
        stringSignatures.emplace(stringSignature);
        hashBasedSignature[hashCode] = stringSignatures;
    }
    else
    {
        hashBasedSignature[hashCode] = {stringSignature};
    }
}

void LogicalOperator::setOperatorState(NES::OperatorState newOperatorState)
{
    using enum OperatorState;
    ///Set the new operator state after validating the previous state
    switch (newOperatorState)
    {
        case TO_BE_PLACED:
            /// an operator can be marked as TO_BE_PLACED only if it is in the state TO_BE_PLACED or TO_BE_REPLACED.
            INVARIANT(
                this->operatorState == TO_BE_PLACED || this->operatorState == TO_BE_REPLACED,
                "Operator with id {} was found in state {} "
                "but expected TO_BE_REMOVED or TO_BE_REPLACED",
                id,
                magic_enum::enum_name(this->operatorState));
            this->operatorState = TO_BE_PLACED;
            break;
        case TO_BE_REMOVED:
            /// an operator can be marked as TO_BE_REMOVED only if it is not in the state REMOVED.
            INVARIANT(
                this->operatorState != REMOVED,
                "Operator with id {} was found in state {} "
                "but expected TO_BE_REMOVED, TO_BE_PLACED, PLACED or TO_BE_REPLACED",
                id,
                magic_enum::enum_name(this->operatorState));
            this->operatorState = TO_BE_REMOVED;
            break;
        case TO_BE_REPLACED:
            /// an operator can be marked as TO_BE_REPLACED only if it is not in the state REMOVED or TO_BE_REMOVED.
            INVARIANT(
                this->operatorState != REMOVED && this->operatorState != TO_BE_REMOVED,
                "Operator with id {} was found in state {} "
                "but expected TO_BE_PLACED, PLACED or TO_BE_REPLACED",
                id,
                magic_enum::enum_name(this->operatorState));
            this->operatorState = TO_BE_REPLACED;
            break;
        case PLACED:
            /// an operator can be marked as PLACED only if it is not in the state REMOVED or TO_BE_REMOVED or already PLACED.
            INVARIANT(
                this->operatorState != REMOVED && this->operatorState != TO_BE_REMOVED,
                "Operator with id {} was found in state {} "
                "but expected TO_BE_PLACED or TO_BE_REPLACED",
                id,
                magic_enum::enum_name(this->operatorState));
            this->operatorState = PLACED;
            break;
        case REMOVED:
            /// an operator can be marked as REMOVED only if it is in the state TO_BE_REMOVED.
            INVARIANT(
                this->operatorState == TO_BE_PLACED || this->operatorState == TO_BE_REMOVED,
                "Operator with id {} was found in state {} "
                "but expected TO_BE_PLACED or TO_BE_REMOVED",
                id,
                magic_enum::enum_name(this->operatorState));
            this->operatorState = REMOVED;
            break;
    }
}

OperatorState LogicalOperator::getOperatorState() const
{
    return operatorState;
}

}
