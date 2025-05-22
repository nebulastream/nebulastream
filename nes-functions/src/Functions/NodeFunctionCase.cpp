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
#include <algorithm>
#include <iterator>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/NodeFunction.hpp>
#include <Functions/NodeFunctionCase.hpp>
#include <Functions/NodeFunctionWhen.hpp>
#include <Nodes/Node.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{
NodeFunctionCase::NodeFunctionCase(std::shared_ptr<DataType> stamp) : NodeFunction(std::move(stamp), "Case")
{
}

NodeFunctionCase::NodeFunctionCase(NodeFunctionCase* other) : NodeFunction(other)
{
    auto otherWhenChildren = getWhenChildren();
    for (auto& whenItr : otherWhenChildren)
    {
        addChildWithEqual(whenItr->deepCopy());
    }
    addChildWithEqual(getDefaultExp()->deepCopy());
}

std::shared_ptr<NodeFunction>
NodeFunctionCase::create(const std::vector<std::shared_ptr<NodeFunction>>& whenExps, const std::shared_ptr<NodeFunction>& defaultExp)
{
    auto caseNode = std::make_shared<NodeFunctionCase>(defaultExp->getStamp());
    caseNode->setChildren(whenExps, defaultExp);
    return caseNode;
}

void NodeFunctionCase::inferStamp(const Schema& schema)
{
    auto whenChildren = getWhenChildren();
    auto defaultExp = getDefaultExp();
    defaultExp->inferStamp(schema);
    INVARIANT(
        !NES::Util::instanceOf<Undefined>(defaultExp->getStamp()),
        "Error during stamp inference. Right type must be defined, but was: {}",
        defaultExp->getStamp()->toString());

    for (auto elem : whenChildren)
    {
        elem->inferStamp(schema);
        ///all elements in whenChildren must be Whens
        INVARIANT(
            NES::Util::instanceOf<NodeFunctionWhen>(elem),
            "Error during stamp inference. All functions in when function vector must be when functions, but {} is not a when function.",
            *elem);
        ///all elements must have same stamp as defaultExp value
        INVARIANT(
            *defaultExp->getStamp() == *elem->getStamp(),
            "Error during stamp inference. All elements must have same stamp as defaultExp default value, but element {} has: {}. Right "
            "was: {}",
            *elem,
            elem->getStamp()->toString(),
            defaultExp->getStamp()->toString());
    }

    stamp = defaultExp->getStamp();
    NES_TRACE("NodeFunctionCase: we assigned the following stamp: {}", stamp->toString());
}

void NodeFunctionCase::setChildren(
    const std::vector<std::shared_ptr<NodeFunction>>& whenExps, const std::shared_ptr<NodeFunction>& defaultExp)
{
    for (auto elem : whenExps)
    {
        addChildWithEqual(elem);
    }
    addChildWithEqual(defaultExp);
}

std::vector<std::shared_ptr<NodeFunction>> NodeFunctionCase::getWhenChildren() const
{
    if (children.size() < 2)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
    std::vector<std::shared_ptr<NodeFunction>> whenChildren;
    for (auto whenIter = children.begin(); whenIter != children.end() - 1; ++whenIter)
    {
        whenChildren.push_back(Util::as<NodeFunction>(*whenIter));
    }
    return whenChildren;
}

std::shared_ptr<NodeFunction> NodeFunctionCase::getDefaultExp() const
{
    if (children.size() <= 1)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
    return Util::as<NodeFunction>(*(children.end() - 1));
}

bool NodeFunctionCase::equal(const std::shared_ptr<Node>& rhs) const
{
    if (NES::Util::instanceOf<NodeFunctionCase>(rhs))
    {
        auto otherCaseNode = NES::Util::as<NodeFunctionCase>(rhs);
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

std::ostream& NodeFunctionCase::toDebugString(std::ostream& os) const
{
    std::vector<std::shared_ptr<NodeFunction>> left = getWhenChildren();
    std::vector<std::string> leftStrs;
    std::ranges::transform(
        left, std::back_inserter(leftStrs), [](const std::shared_ptr<NodeFunction>& func) { return fmt::format("{}", *func); });
    return os << fmt::format("CASE({{{}}}, defaultExp: )", fmt::join(leftStrs, ", "));
}

std::shared_ptr<NodeFunction> NodeFunctionCase::deepCopy()
{
    std::vector<std::shared_ptr<NodeFunction>> copyOfWhenFunctions;
    for (auto whenFunction : getWhenChildren())
    {
        copyOfWhenFunctions.push_back(NES::Util::as<NodeFunction>(whenFunction)->deepCopy());
    }
    return NodeFunctionCase::create(copyOfWhenFunctions, getDefaultExp()->deepCopy());
}

bool NodeFunctionCase::validateBeforeLowering() const
{
    ///NodeFunction Currently, we do not have any validation for Case before lowering
    return true;
}

}
