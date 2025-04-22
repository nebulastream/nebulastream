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

#include <sstream>
#include <string>
#include <Functions/LogicalFunctions/GreaterEqualsBinaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

GreaterEqualsBinaryLogicalFunction::GreaterEqualsBinaryLogicalFunction() noexcept
    : BinaryLogicalFunction(DataTypeFactory::createBoolean(), "GreaterEquals")
{
}

GreaterEqualsBinaryLogicalFunction::GreaterEqualsBinaryLogicalFunction(GreaterEqualsBinaryLogicalFunction* other)
    : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
GreaterEqualsBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto greaterThen = std::make_shared<GreaterEqualsBinaryLogicalFunction>();
    greaterThen->setLeftChild(left);
    greaterThen->setRightChild(right);
    return greaterThen;
}

bool GreaterEqualsBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<GreaterEqualsBinaryLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<GreaterEqualsBinaryLogicalFunction>(rhs);
        return this->getLeftChild()->equal(other->getLeftChild()) && this->getRightChild()->equal(other->getRightChild());
    }
    return false;
}

std::string GreaterEqualsBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << ">=" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> GreaterEqualsBinaryLogicalFunction::clone() const
{
    return GreaterEqualsBinaryLogicalFunction::create(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}
}
