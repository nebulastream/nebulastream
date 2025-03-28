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
#include <utility>
#include <Functions/ArithmeticalFunctions/DivLogicalFunction.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

DivLogicalFunction::DivLogicalFunction(std::shared_ptr<DataType> stamp) : BinaryLogicalFunction(std::move(stamp), "Div") {};

DivLogicalFunction::DivLogicalFunction(DivLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
DivLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto divNode = std::make_shared<DivLogicalFunction>(left->getStamp());
    divNode->setLeftChild(left);
    divNode->setRightChild(right);
    return divNode;
}

bool DivLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<DivLogicalFunction>(rhs))
    {
        auto otherDivNode = NES::Util::as<DivLogicalFunction>(rhs);
        return getLeftChild()->equal(otherDivNode->getLeftChild()) && getRightChild()->equal(otherDivNode->getRightChild());
    }
    return false;
}

std::string DivLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << "/" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> DivLogicalFunction::clone() const
{
    return DivLogicalFunction::create(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}


}
