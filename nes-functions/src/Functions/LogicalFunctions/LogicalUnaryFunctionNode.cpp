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

#include <Functions/LogicalFunctions/LogicalUnaryFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

LogicalUnaryFunctionNode::LogicalUnaryFunctionNode(std::string name) : UnaryFunctionNode(DataTypeFactory::createBoolean(), std::move(name)), LogicalFunctionNode()
{
}

LogicalUnaryFunctionNode::LogicalUnaryFunctionNode(LogicalUnaryFunctionNode* other) : UnaryFunctionNode(other)
{
}

bool LogicalUnaryFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<LogicalUnaryFunctionNode>())
    {
        auto other = rhs->as<LogicalUnaryFunctionNode>();
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}
} /// namespace NES
