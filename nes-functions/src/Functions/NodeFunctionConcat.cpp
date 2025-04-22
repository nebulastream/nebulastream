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
#include <ostream>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionBinary.hpp>
#include <Functions/NodeFunctionConcat.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

bool NodeFunctionConcat::validateBeforeLowering() const
{
    return getLeft()->getStamp().type == DataType::Type::VARSIZED and getRight()->getStamp().type == DataType::Type::VARSIZED;
}

std::shared_ptr<NodeFunction> NodeFunctionConcat::deepCopy()
{
    return create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
}

bool NodeFunctionConcat::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionConcat>(rhs))
    {
        const auto otherMulNode = NES::Util::as<NodeFunctionConcat>(rhs);
        const bool match = getLeft()->equal(otherMulNode->getLeft()) and getRight()->equal(otherMulNode->getRight());
        return match;
    }
    return false;
}
void NodeFunctionConcat::inferStamp(const Schema& schema)
{
    this->getLeft()->inferStamp(schema);
    this->getRight()->inferStamp(schema);
    INVARIANT(
        this->getLeft()->getStamp().isType(DataType::Type::VARSIZED), "The Concat function must have children of type VariableSizedData.");
    INVARIANT(
        this->getLeft()->getStamp().isType(DataType::Type::VARSIZED), "The Concat function must have children of type VariableSizedData.");
    this->stamp = getLeft()->getStamp();
}

NodeFunctionConcat::NodeFunctionConcat(DataType stamp) : NodeFunctionBinary(std::move(stamp), "Concat")
{
}

std::shared_ptr<NodeFunction>
NodeFunctionConcat::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto concatNode = std::make_shared<NodeFunctionConcat>(left->getStamp());
    concatNode->setChildren(left, right);
    return concatNode;
}

std::ostream& NodeFunctionConcat::toDebugString(std::ostream& os) const
{
    return os << fmt::format("CONCAT({} ({}, {}))", this->stamp, *getLeft(), *getRight());
}

}
