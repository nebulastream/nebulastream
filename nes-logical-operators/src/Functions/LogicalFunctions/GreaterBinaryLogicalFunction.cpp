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

#include <Functions/LogicalFunctions/GreaterBinaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

GreaterBinaryLogicalFunction::GreaterBinaryLogicalFunction() noexcept : BinaryLogicalFunction(DataTypeFactory::createBoolean(), "Greater")
{
}

GreaterBinaryLogicalFunction::GreaterBinaryLogicalFunction(GreaterBinaryLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
GreaterBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto greater = std::make_shared<GreaterBinaryLogicalFunction>();
    greater->setLeftChild(left);
    greater->setRightChild(right);
    return greater;
}

bool GreaterBinaryLogicalFunction::equal(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<GreaterBinaryLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<GreaterBinaryLogicalFunction>(rhs);
        return this->getLeftChild()->equal(other->getLeftChild()) && this->getRightChild()->equal(other->getRightChild());
    }
    return false;
}

std::string GreaterBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *getLeftChild() << ">" << *getRightChild();
    return ss.str();
}

std::shared_ptr<LogicalFunction> GreaterBinaryLogicalFunction::clone() const
{
    return GreaterBinaryLogicalFunction::create(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

}
