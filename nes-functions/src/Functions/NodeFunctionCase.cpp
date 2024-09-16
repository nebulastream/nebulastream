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
#include <Functions/NodeFunctionCase.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
namespace NES
{
NodeFunctionCase::NodeFunctionCase(DataTypePtr stamp) : FunctionNode(std::move(stamp), "Case")
{
}

NodeFunctionCase::NodeFunctionCase(NodeFunctionCase* other) : FunctionNode(other)
{
    auto otherWhenChildren = getWhenChildren();
    for (auto& whenItr : otherWhenChildren)
    {
        addChildWithEqual(whenItr->deepCopy());
    }
    addChildWithEqual(getDefaultExp()->deepCopy());
}

NodeFunctionPtr NodeFunctionCase::create(std::vector<NodeFunctionPtr> const& whenExps, const NodeFunctionPtr& defaultExp)
{
    auto caseNode = std::make_shared<NodeFunctionCase>(defaultExp->getStamp());
    caseNode->setChildren(whenExps, defaultExp);
    return caseNode;
}

void NodeFunctionCase::inferStamp(SchemaPtr schema)
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
        ///NodeFunctionall elements in whenChildren must be Whens
        if (!elem->instanceOf<NodeFunctionWhen>())
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
    NES_TRACE("NodeFunctionCase: we assigned the following stamp: {}", stamp->toString());
}

void NodeFunctionCase::setChildren(std::vector<NodeFunctionPtr> const& whenExps, NodeFunctionPtr const& defaultExp)
{
    for (auto elem : whenExps)
    {
        addChildWithEqual(elem);
    }
    addChildWithEqual(defaultExp);
}

std::vector<NodeFunctionPtr> NodeFunctionCase::getWhenChildren() const
{
    if (children.size() < 2)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
    std::vector<NodeFunctionPtr> whenChildren;
    for (auto whenIter = children.begin(); whenIter != children.end() - 1; ++whenIter)
    {
        whenChildren.push_back(whenIter->get()->as<FunctionNode>());
    }
    return whenChildren;
}

NodeFunctionPtr NodeFunctionCase::getDefaultExp() const
{
    if (children.size() <= 1)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
    return (*(children.end() - 1))->as<FunctionNode>();
}

bool NodeFunctionCase::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<NodeFunctionCase>())
    {
        auto otherCaseNode = rhs->as<NodeFunctionCase>();
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

std::string NodeFunctionCase::toString() const
{
    std::stringstream ss;
    ss << "CASE({";
    std::vector<NodeFunctionPtr> left = getWhenChildren();
    for (std::size_t i = 0; i < left.size() - 1; i++)
    {
        ss << left.at(i)->toString() << ",";
    }
    ss << (*(left.end() - 1))->toString() << "}," << getDefaultExp()->toString();

    return ss.str();
}

NodeFunctionPtr NodeFunctionCase::deepCopy()
{
    std::vector<NodeFunctionPtr> copyOfWhenFunctions;
    for (auto whenFunction : getWhenChildren())
    {
        copyOfWhenFunctions.push_back(whenFunction->as<FunctionNode>()->deepCopy());
    }
    return NodeFunctionCase::create(copyOfWhenFunctions, getDefaultExp()->deepCopy());
}

bool NodeFunctionCase::validateBeforeLowering() const
{
    ///NodeFunction Currently, we do not have any validation for Case before lowering
    return true;
}

}
