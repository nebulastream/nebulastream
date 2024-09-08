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
#include <Functions/CaseFunctionNode.hpp>
#include <Functions/WhenFunctionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/DataTypes/DataType.hpp>
namespace NES
{
CaseFunctionNode::CaseFunctionNode(DataTypePtr stamp) : FunctionNode(std::move(stamp))
{
}

CaseFunctionNode::CaseFunctionNode(CaseFunctionNode* other) : FunctionNode(other)
{
    auto otherWhenChildren = getWhenChildren();
    for (auto& whenItr : otherWhenChildren)
    {
        addChildWithEqual(whenItr->copy());
    }
    addChildWithEqual(getDefaultExp()->copy());
}

FunctionNodePtr CaseFunctionNode::create(std::vector<FunctionNodePtr> const& whenExps, const FunctionNodePtr& defaultExp)
{
    auto caseNode = std::make_shared<CaseFunctionNode>(defaultExp->getStamp());
    caseNode->setChildren(whenExps, defaultExp);
    return caseNode;
}

void CaseFunctionNode::inferStamp(SchemaPtr schema)
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
        ///all elements in whenChildren must be WhenFunctionNodes
        if (!elem->instanceOf<WhenFunctionNode>())
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
    NES_TRACE("CaseFunctionNode: we assigned the following stamp: {}", stamp->toString());
}

void CaseFunctionNode::setChildren(std::vector<FunctionNodePtr> const& whenExps, FunctionNodePtr const& defaultExp)
{
    for (auto elem : whenExps)
    {
        addChildWithEqual(elem);
    }
    addChildWithEqual(defaultExp);
}

std::vector<FunctionNodePtr> CaseFunctionNode::getWhenChildren() const
{
    if (children.size() < 2)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
    std::vector<FunctionNodePtr> whenChildren;
    for (auto whenIter = children.begin(); whenIter != children.end() - 1; ++whenIter)
    {
        whenChildren.push_back(whenIter->get()->as<FunctionNode>());
    }
    return whenChildren;
}

FunctionNodePtr CaseFunctionNode::getDefaultExp() const
{
    if (children.size() <= 1)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
    return (*(children.end() - 1))->as<FunctionNode>();
}

bool CaseFunctionNode::equal(NodePtr const& rhs) const
{
    if (rhs->instanceOf<CaseFunctionNode>())
    {
        auto otherCaseNode = rhs->as<CaseFunctionNode>();
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

std::string CaseFunctionNode::toString() const
{
    std::stringstream ss;
    ss << "CASE({";
    std::vector<FunctionNodePtr> left = getWhenChildren();
    for (std::size_t i = 0; i < left.size() - 1; i++)
    {
        ss << left.at(i)->toString() << ",";
    }
    ss << (*(left.end() - 1))->toString() << "}," << getDefaultExp()->toString();

    return ss.str();
}

FunctionNodePtr CaseFunctionNode::copy()
{
    std::vector<FunctionNodePtr> copyOfWhenFunctions;
    for (auto whenFunction : getWhenChildren())
    {
        copyOfWhenFunctions.push_back(whenFunction->as<FunctionNode>()->copy());
    }
    return CaseFunctionNode::create(copyOfWhenFunctions, getDefaultExp()->copy());
}

} /// namespace NES
