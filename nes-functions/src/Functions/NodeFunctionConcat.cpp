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

#include <Functions/NodeFunctionConcat.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

namespace NES {

bool NodeFunctionConcat::validateBeforeLowering() const
{
    return NES::Util::instanceOf<VariableSizedDataType>(getLeft()->getStamp()) and NES::Util::instanceOf<VariableSizedDataType>(getRight()->getStamp());
}

std::shared_ptr<NodeFunction> NodeFunctionConcat::deepCopy()
{
    return NodeFunctionConcat::create(Util::as<NodeFunction>(children[0])->deepCopy(), Util::as<NodeFunction>(children[1])->deepCopy());
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

NodeFunctionConcat::NodeFunctionConcat(std::shared_ptr<DataType> stamp)  : NodeFunctionBinary(std::move(stamp), "Concat") {}

std::shared_ptr<NodeFunction>
NodeFunctionConcat::create(const std::shared_ptr<NodeFunction>& left, const std::shared_ptr<NodeFunction>& right)
{
    auto concatNode = std::make_shared<NodeFunctionConcat>(left->getStamp());
    concatNode->setChildren(left, right);
    return concatNode;
}

std::string NodeFunctionConcat::toString() const
{
    return fmt::format("Concat({}, {})", *getLeft(), *getRight());

}

}