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

#include <Functions/NodeFunctionCastToType.hpp>

namespace NES {

std::shared_ptr<NodeFunction> NodeFunctionCastToType::create(std::shared_ptr<DataType> castToType)
{
    return std::make_shared<NodeFunctionCastToType>(NodeFunctionCastToType(castToType, "FieldCast"));
}

bool NodeFunctionCastToType::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionCastToType>(rhs))
    {
        const auto otherNode = NES::Util::as<NodeFunctionCastToType>(rhs);
        const bool match = otherNode->castToType == this->castToType;
        return match;
    }
    return false;
}

bool NodeFunctionCastToType::validateBeforeLowering() const
{
    return not children.empty();
}

std::shared_ptr<DataType> NodeFunctionCastToType::getCastToType() const
{
    return castToType;
}

NodeFunctionCastToType::NodeFunctionCastToType(std::shared_ptr<DataType> castToType, std::string name)
    : NodeFunctionUnary(castToType, name), castToType(castToType)
{
}

void NodeFunctionCastToType::inferStamp(const Schema&)
{
    stamp = castToType;
}
std::shared_ptr<NodeFunction> NodeFunctionCastToType::deepCopy()
{
    return std::make_shared<NodeFunctionCastToType>(*this);
}

NodeFunctionCastToType::NodeFunctionCastToType(NodeFunctionCastToType* other) : NodeFunctionUnary(other), castToType(other->castToType)
{
}

std::ostream& NodeFunctionCastToType::toDebugString(std::ostream& os) const
{
    return os << std::format("Cast to {}", castToType->toString());
}

std::ostream& NodeFunctionCastToType::toQueryPlanString(std::ostream& os) const
{
    return os << std::format("Cast to {}", castToType->toString());

}

}