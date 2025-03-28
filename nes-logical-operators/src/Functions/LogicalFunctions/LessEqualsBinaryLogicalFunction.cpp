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

#include <Functions/LogicalFunctions/LessEqualsBinaryLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

LessEqualsBinaryLogicalFunction::LessEqualsBinaryLogicalFunction() : BinaryLogicalFunction("LessEquals")
{
}

LessEqualsBinaryLogicalFunction::LessEqualsBinaryLogicalFunction(LessEqualsBinaryLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
LessEqualsBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto lessThen = std::make_shared<LessEqualsBinaryLogicalFunction>();
    lessThen->setChildren(left, right);
    return lessThen;
}

bool LessEqualsBinaryLogicalFunction::equal(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<LessEqualsBinaryLogicalFunction>(rhs))
    {
        auto other = NES::Util::as<LessEqualsBinaryLogicalFunction>(rhs);
        return this->getLeft()->equal(other->getLeft()) && this->getRight()->equal(other->getRight());
    }
    return false;
}

std::string LessEqualsBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << *children[0] << "<=" << *children[1];
    return ss.str();
}

std::shared_ptr<LogicalFunction> LessEqualsBinaryLogicalFunction::deepCopy()
{
    return LessEqualsBinaryLogicalFunction::create(
        Util::as<LogicalFunction>(children[0])->deepCopy(), Util::as<LogicalFunction>(children[1])->deepCopy());
}

}
