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
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/WhenBinaryLogicalFunction.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>


namespace NES
{
WhenBinaryLogicalFunction::WhenBinaryLogicalFunction(std::shared_ptr<DataType> stamp) : BinaryLogicalFunction(std::move(stamp), "When") {};

WhenBinaryLogicalFunction::WhenBinaryLogicalFunction(WhenBinaryLogicalFunction* other) : BinaryLogicalFunction(other)
{
}

std::shared_ptr<LogicalFunction>
WhenBinaryLogicalFunction::create(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)
{
    auto whenNode = std::make_shared<WhenBinaryLogicalFunction>(left->getStamp());
    whenNode->setChildren(left, right);
    return whenNode;
}

void WhenBinaryLogicalFunction::inferStamp(const Schema& schema)
{
    auto left = getLeft();
    auto right = getRight();
    left->inferStamp(schema);
    right->inferStamp(schema);

    ///left function has to be boolean
    if (!NES::Util::instanceOf<Boolean>(left->getStamp()))
    {
        CannotInferStamp(
            "Error during stamp inference. Left type needs to be Boolean, but Left was: " + left->getStamp()->toString()
            + "Right was: " + right->getStamp()->toString());
    }

    ///set stamp to right stamp, as only the left function will be returned
    stamp = right->getStamp();
    NES_TRACE("WhenBinaryLogicalFunction: we assigned the following stamp: {}", stamp->toString())
}

bool WhenBinaryLogicalFunction::equal(const std::shared_ptr<LogicalFunction>& rhs) const
{
    if (NES::Util::instanceOf<WhenBinaryLogicalFunction>(rhs))
    {
        auto otherWhenNode = NES::Util::as<WhenBinaryLogicalFunction>(rhs);
        return getLeft()->equal(otherWhenNode->getLeft()) && getRight()->equal(otherWhenNode->getRight());
    }
    return false;
}

std::string WhenBinaryLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "WHEN(" << *children[0] << "," << *children[1] << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> WhenBinaryLogicalFunction::deepCopy()
{
    return WhenBinaryLogicalFunction::create(
        Util::as<LogicalFunction>(children[0])->deepCopy(), Util::as<LogicalFunction>(children[1])->deepCopy());
}

bool WhenBinaryLogicalFunction::validateBeforeLowering() const
{
    /// left function has to be boolean
    const auto functionLeft = this->getLeft();
    return NES::Util::instanceOf<Boolean>(functionLeft->getStamp());
}

}
