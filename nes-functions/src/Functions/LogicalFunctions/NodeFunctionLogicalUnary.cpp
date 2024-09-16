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

#include <Functions/LogicalFunctions/NodeFunctionLogicalUnary.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{

NodeFunctionLogicalUnary::NodeFunctionLogicalUnary(std::string name) : NodeFunctionUnary(DataTypeFactory::createBoolean(), std::move(name)), LogicalFunctionNode()
{
}

NodeFunctionLogicalUnary::NodeFunctionLogicalUnary(NodeFunctionLogicalUnary* other) : NodeFunctionUnary(other)
{
}

bool NodeFunctionLogicalUnary::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NodeFunctionLogicalUnary>())
    {
        auto other = rhs->as<NodeFunctionLogicalUnary>();
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}
} /// namespace NES
