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

#include <span>
#include <atomic>
#include <ostream>
#include <Identifiers/Identifiers.hpp>
#include <Plans/Operator.hpp>

namespace NES
{

namespace
{
OperatorId getNextOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}
}

Operator::Operator() : id(getNextOperatorId())
{
}

bool Operator::operator==(const Operator& rhs) const
{
    const Operator& leftRef = *this;
    const Operator& rightRef = rhs;
    if (typeid(leftRef) != typeid(rightRef)) {
        return false;
    }
    if (children.size() != rhs.children.size()) {
        return false;
    }
    for (size_t i = 0; i < children.size(); ++i) {
        if (!children[i] || !rhs.children[i]) {
            if (children[i] != rhs.children[i]) {
                return false;
            }
        }
        else if (!(*children[i] == *rhs.children[i])) {
            return false;
        }
    }
    return true;
}

std::span<const std::shared_ptr<Operator>> Operator::getChildren() const
{
    return children;
}

std::ostream& operator<<(std::ostream& os, const Operator& op)
{
    os << op.toString();
    return os;
}

}
