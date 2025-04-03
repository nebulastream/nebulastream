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
#include <Functions/LogicalFunctions/NodeFunctionLogicalUnary.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>
#include <Common/DataTypes/Numeric.hpp>
#include "Functions/LogicalFunctions/NodeFunctionLogical.hpp"
#include "Functions/NodeFunctionUnary.hpp"


namespace NES
{

NodeFunctionLogicalUnary::NodeFunctionLogicalUnary(std::string name)
    : NodeFunctionUnary(DataTypeProvider::provideDataType(LogicalType::BOOLEAN, DataTypeProvider::isNullable(name)), std::move(name))
    , LogicalNodeFunction()
{
}

NodeFunctionLogicalUnary::NodeFunctionLogicalUnary(NodeFunctionLogicalUnary* other) : NodeFunctionUnary(other)
{
}

bool NodeFunctionLogicalUnary::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionLogicalUnary>(rhs))
    {
        auto other = NES::Util::as<NodeFunctionLogicalUnary>(rhs);
        return this->getChildren()[0]->equal(other->getChildren()[0]);
    }
    return false;
}
}
