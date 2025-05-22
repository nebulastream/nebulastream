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
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionUnary.hpp>
#include <Util/Common.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
NodeFunctionUnary::NodeFunctionUnary(std::shared_ptr<DataType> stamp, std::string name) : NodeFunction(std::move(stamp), std::move(name))
{
}

NodeFunctionUnary::NodeFunctionUnary(NodeFunctionUnary* other) : NodeFunction(other)
{
}

void NodeFunctionUnary::setChild(const std::shared_ptr<NodeFunction>& child)
{
    addChildWithEqual(child);
}

std::shared_ptr<NodeFunction> NodeFunctionUnary::child() const
{
    return Util::as<NodeFunction>(children[0]);
}
}
