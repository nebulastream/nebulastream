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
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
#include <Functions/NodeFunctionCase.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Util/Common.hpp>
========
#include <Functions/CaseFunctionNode.hpp>
#include <Functions/WhenFunctionNode.hpp>
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
NodeFunctionCase::NodeFunctionCase(DataTypePtr stamp) : NodeFunction(std::move(stamp), "Case")
{
}

NodeFunctionCase::NodeFunctionCase(NodeFunctionCase* other) : NodeFunction(other)
========
CaseFunctionNode::CaseFunctionNode(DataTypePtr stamp) : FunctionNode(std::move(stamp))
{
}

CaseFunctionNode::CaseFunctionNode(CaseFunctionNode* other) : FunctionNode(other)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
{
    auto otherWhenChildren = getWhenChildren();
    for (auto& whenItr : otherWhenChildren)
    {
        addChildWithEqual(whenItr->deepCopy());
    }
    addChildWithEqual(getDefaultExp()->deepCopy());
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
NodeFunctionPtr NodeFunctionCase::create(std::vector<NodeFunctionPtr> const& whenExps, const NodeFunctionPtr& defaultExp)
{
    auto caseNode = std::make_shared<NodeFunctionCase>(defaultExp->getStamp());
========
FunctionNodePtr CaseFunctionNode::create(std::vector<FunctionNodePtr> const& whenExps, const FunctionNodePtr& defaultExp)
{
    auto caseNode = std::make_shared<CaseFunctionNode>(defaultExp->getStamp());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
    caseNode->setChildren(whenExps, defaultExp);
    return caseNode;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
void NodeFunctionCase::inferStamp(SchemaPtr schema)
========
void CaseFunctionNode::inferStamp(SchemaPtr schema)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
{
    auto whenChildren = getWhenChildren();
    auto defaultExp = getDefaultExp();
    defaultExp->inferStamp(schema);
    if (defaultExp->getStamp()->isUndefined())
    {
        NES_THROW_RUNTIME_ERROR(
            "Error during stamp inference. Right type must be defined, but was: {}", defaultExp->getStamp()->toString());
    }

    for (auto elem : whenChildren)
    {
        elem->inferStamp(schema);
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
        ///NodeFunctionall elements in whenChildren must be Whens
        if (!NES::Util::instanceOf<NodeFunctionWhen>(elem))
========
        ///all elements in whenChildren must be WhenFunctionNodes
        if (!elem->instanceOf<WhenFunctionNode>())
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
        {
            NES_THROW_RUNTIME_ERROR(
                "Error during stamp inference. All functions in when function vector must be when functions, but " + elem->toString()
                + " is not a when function.");
        }
        ///all elements must have same stamp as defaultExp value
        if (!defaultExp->getStamp()->equals(elem->getStamp()))
        {
            NES_THROW_RUNTIME_ERROR(
                "Error during stamp inference. All elements must have same stamp as defaultExp default value, but element "
                + elem->toString() + " has: " + elem->getStamp()->toString() + ". Right was: " + defaultExp->getStamp()->toString());
        }
    }

    stamp = defaultExp->getStamp();
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
    NES_TRACE("NodeFunctionCase: we assigned the following stamp: {}", stamp->toString());
}

void NodeFunctionCase::setChildren(std::vector<NodeFunctionPtr> const& whenExps, NodeFunctionPtr const& defaultExp)
========
    NES_TRACE("CaseFunctionNode: we assigned the following stamp: {}", stamp->toString());
}

void CaseFunctionNode::setChildren(std::vector<FunctionNodePtr> const& whenExps, FunctionNodePtr const& defaultExp)
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
{
    for (auto elem : whenExps)
    {
        addChildWithEqual(elem);
    }
    addChildWithEqual(defaultExp);
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
std::vector<NodeFunctionPtr> NodeFunctionCase::getWhenChildren() const
========
std::vector<FunctionNodePtr> CaseFunctionNode::getWhenChildren() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
{
    if (children.size() < 2)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
    std::vector<NodeFunctionPtr> whenChildren;
    for (auto whenIter = children.begin(); whenIter != children.end() - 1; ++whenIter)
    {
        whenChildren.push_back(Util::as<NodeFunction>(*whenIter));
========
    std::vector<FunctionNodePtr> whenChildren;
    for (auto whenIter = children.begin(); whenIter != children.end() - 1; ++whenIter)
    {
        whenChildren.push_back(whenIter->get()->as<FunctionNode>());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
    }
    return whenChildren;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
NodeFunctionPtr NodeFunctionCase::getDefaultExp() const
========
FunctionNodePtr CaseFunctionNode::getDefaultExp() const
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
{
    if (children.size() <= 1)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
    return Util::as<NodeFunction>(*(children.end() - 1));
}

bool NodeFunctionCase::equal(NodePtr const& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionCase>(rhs))
    {
        auto otherCaseNode = NES::Util::as<NodeFunctionCase>(rhs);
========
    return (*(children.end() - 1))->as<FunctionNode>();
}

bool CaseFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<CaseFunctionNode>())
    {
        auto otherCaseNode = rhs->as<CaseFunctionNode>();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
        if (getChildren().size() != otherCaseNode->getChildren().size())
        {
            return false;
        }
        for (std::size_t i = 0; i < getChildren().size(); i++)
        {
            if (!getChildren().at(i)->equal(otherCaseNode->getChildren().at(i)))
            {
                return false;
            }
        }
        return true;
    }
    return false;
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
std::string NodeFunctionCase::toString() const
{
    std::stringstream ss;
    ss << "CASE({";
    std::vector<NodeFunctionPtr> left = getWhenChildren();
========
std::string CaseFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "CASE({";
    std::vector<FunctionNodePtr> left = getWhenChildren();
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
    for (std::size_t i = 0; i < left.size() - 1; i++)
    {
        ss << left.at(i)->toString() << ",";
    }
    ss << (*(left.end() - 1))->toString() << "}," << getDefaultExp()->toString();

    return ss.str();
}

<<<<<<<< HEAD:nes-functions/src/Functions/NodeFunctionCase.cpp
NodeFunctionPtr NodeFunctionCase::deepCopy()
{
    std::vector<NodeFunctionPtr> copyOfWhenFunctions;
    for (auto whenFunction : getWhenChildren())
    {
        copyOfWhenFunctions.push_back(NES::Util::as<NodeFunction>(whenFunction)->deepCopy());
    }
    return NodeFunctionCase::create(copyOfWhenFunctions, getDefaultExp()->deepCopy());
========
FunctionNodePtr CaseFunctionNode::copy()
{
    std::vector<FunctionNodePtr> copyOfWhenFunctions;
    for (auto whenFunction : getWhenChildren())
    {
        copyOfWhenFunctions.push_back(whenFunction->as<FunctionNode>()->copy());
    }
    return CaseFunctionNode::create(copyOfWhenFunctions, getDefaultExp()->copy());
>>>>>>>> 29ee9426db (chore(Expressions/Functions) Renamed expression to function):nes-functions/src/Functions/CaseFunctionNode.cpp
}

bool NodeFunctionCase::validateBeforeLowering() const
{
    ///NodeFunction Currently, we do not have any validation for Case before lowering
    return true;
}

}
