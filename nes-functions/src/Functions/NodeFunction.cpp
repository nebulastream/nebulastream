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

#include <string>
#include <string_view>
#include <utility>
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunction.cpp
#include <Functions/NodeFunction.hpp>
#include <Util/Common.hpp>
========
#include <Functions/FunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FunctionNode.cpp
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunction.cpp
NodeFunction::NodeFunction(DataTypePtr stamp, std::string name) : stamp(std::move(stamp)), name(std::move(name))
{
}

bool NodeFunction::isPredicate() const
========
FunctionNode::FunctionNode(DataTypePtr stamp) : stamp(std::move(stamp))
{
}

bool FunctionNode::isPredicate() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FunctionNode.cpp
{
    return stamp->isBoolean();
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunction.cpp
DataTypePtr NodeFunction::getStamp() const
========
DataTypePtr FunctionNode::getStamp() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FunctionNode.cpp
{
    return stamp;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunction.cpp
void NodeFunction::setStamp(DataTypePtr stamp)
========
void FunctionNode::setStamp(DataTypePtr stamp)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FunctionNode.cpp
{
    this->stamp = std::move(stamp);
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunction.cpp
const std::string& NodeFunction::getName() const
{
    return name;
}

void NodeFunction::inferStamp(SchemaPtr schema)
========
void FunctionNode::inferStamp(SchemaPtr schema)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FunctionNode.cpp
{
    /// infer stamp on all children nodes
    for (const auto& node : children)
    {
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunction.cpp
        NES::Util::as<NodeFunction>(node)->inferStamp(schema);
    }
}

NodeFunction::NodeFunction(const NodeFunction* other) : stamp(other->stamp)
========
        node->as<FunctionNode>()->inferStamp(schema);
    }
}

FunctionNode::FunctionNode(const FunctionNode* other) : stamp(other->stamp)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/FunctionNode.cpp
{
}

}
