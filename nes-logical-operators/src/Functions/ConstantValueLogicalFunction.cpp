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
#include <string>
#include <utility>
#include <Abstract/LogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <fmt/format.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
ConstantValueLogicalFunction::ConstantValueLogicalFunction(const std::shared_ptr<DataType>& type, std::string&& value)
    : LogicalFunction(type, "ConstantValue"), constantValue(std::move(value))
{
}

ConstantValueLogicalFunction::ConstantValueLogicalFunction(const ConstantValueLogicalFunction* other)
    : LogicalFunction(other->getStamp(), "ConstantValue"), constantValue(other->constantValue)
{
}

bool ConstantValueLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (Util::instanceOf<ConstantValueLogicalFunction>(rhs))
    {
        auto otherConstantValueNode = Util::as<ConstantValueLogicalFunction>(rhs);
        return otherConstantValueNode->stamp == stamp && constantValue == otherConstantValueNode->constantValue;
    }
    return false;
}

std::string ConstantValueLogicalFunction::toString() const
{
    return fmt::format("ConstantValue({}, {})", constantValue, stamp->toString());
}

std::shared_ptr<LogicalFunction> ConstantValueLogicalFunction::create(const std::shared_ptr<DataType>& type, std::string value)
{
    return std::make_shared<ConstantValueLogicalFunction>(ConstantValueLogicalFunction(type, std::move(value)));
}

std::string ConstantValueLogicalFunction::getConstantValue() const
{
    return constantValue;
}

void ConstantValueLogicalFunction::inferStamp(std::shared_ptr<Schema>)
{
    /// the stamp of constant value functions is defined by the constant value type.
    /// thus ut is already assigned correctly when the function node is created.
}

std::shared_ptr<LogicalFunction> ConstantValueLogicalFunction::deepCopy()
{
    return std::make_shared<ConstantValueLogicalFunction>(*this);
}

bool ConstantValueLogicalFunction::validateBeforeLowering() const
{
    return children.empty();
}

}
