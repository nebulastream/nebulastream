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
#include <Functions/LogicalFunction.hpp>
#include <Functions/WhenLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Boolean.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

WhenLogicalFunction::WhenLogicalFunction(const WhenLogicalFunction& other) : BinaryLogicalFunction(other)
{
}

WhenLogicalFunction::WhenLogicalFunction(const std::shared_ptr<LogicalFunction>& left, const std::shared_ptr<LogicalFunction>& right)  : BinaryLogicalFunction(std::move(stamp), "When")
{
    this->setLeftChild(left);
    this->setRightChild(right);
}

void WhenLogicalFunction::inferStamp(const Schema& schema)
{
    auto left = getLeftChild();
    auto right = getRightChild();
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
    NES_TRACE("WhenLogicalFunction: we assigned the following stamp: {}", stamp->toString())
}

bool WhenLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<WhenLogicalFunction>(rhs))
    {
        auto otherWhenNode = NES::Util::as<WhenLogicalFunction>(rhs);
        return getLeftChild() == otherWhenNode->getLeftChild()  && getRightChild() == otherWhenNode->getRightChild();
    }
    return false;
}

std::string WhenLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "WHEN(" << *getLeftChild() << "," << *getRightChild() << ")";
    return ss.str();
}

std::shared_ptr<LogicalFunction> WhenLogicalFunction::clone() const
{
    return std::make_shared<WhenLogicalFunction>(getLeftChild()->clone(), Util::as<LogicalFunction>(getRightChild())->clone());
}

bool WhenLogicalFunction::validateBeforeLowering() const
{
    /// left function has to be boolean
    const auto functionLeft = this->getLeftChild();
    return NES::Util::instanceOf<Boolean>(functionLeft->getStamp());
}

}
