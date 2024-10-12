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

#include <utility>
#include <Functions/FunctionNode.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
FunctionNode::FunctionNode(DataTypePtr stamp) : stamp(std::move(stamp))
{
}

bool FunctionNode::isPredicate() const
{
    return stamp->isBoolean();
}

DataTypePtr FunctionNode::getStamp() const
{
    return stamp;
}

void FunctionNode::setStamp(DataTypePtr stamp)
{
    this->stamp = std::move(stamp);
}

void FunctionNode::inferStamp(SchemaPtr schema)
{
    /// infer stamp on all children nodes
    for (const auto& node : children)
    {
        NES::Util::as<FunctionNode>(node)->inferStamp(schema);
    }
}

FunctionNode::FunctionNode(const FunctionNode* other) : stamp(other->stamp)
{
}

} /// namespace NES
