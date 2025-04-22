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
#include <Nodes/Node.hpp>
#include <Operators/LogicalOperators/LimitLogicalOperator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{

LimitLogicalOperator::LimitLogicalOperator(uint64_t limit, OperatorId id) : Operator(id), UnaryLogicalOperator(id), limit(limit)
{
}

uint64_t LimitLogicalOperator::getLimit() const
{
    return limit;
}

bool LimitLogicalOperator::isIdentical(const Operator& rhs) const
{
    return *this == rhs && dynamic_cast<const LimitLogicalOperator*>(&rhs)->getId() == id;
}

bool LimitLogicalOperator::operator==(const Operator& rhs) const
{
    if (const auto rhsOperator = dynamic_cast<const LimitLogicalOperator*>(&rhs))
    {
        return limit == rhsOperator->limit;
    }
    return false;
};

std::string LimitLogicalOperator::toString() const
{
    std::stringstream ss;
    ss << "LIMIT" << id << ")";
    return ss.str();
}

bool LimitLogicalOperator::inferSchema()
{
    if (!UnaryLogicalOperator::inferSchema())
    {
        return false;
    }
    return true;
}

std::shared_ptr<Operator> LimitLogicalOperator::clone() const
{
    auto copy = std::make_shared<LimitLogicalOperator>(limit, id);
    copy->setInputOriginIds(inputOriginIds);
    copy->setInputSchema(inputSchema);
    copy->setOutputSchema(outputSchema);
    for (const auto& [key, value] : properties)
    {
        copy->addProperty(key, value);
    }
    return copy;
}
}
