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
#include <sstream>
#include <utility>
#include <vector>
#include <API/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/CaseLogicalFunction.hpp>
#include <Functions/WhenLogicalFunction.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/Undefined.hpp>

namespace NES
{
CaseLogicalFunction::CaseLogicalFunction(std::shared_ptr<DataType> stamp) : LogicalFunction(std::move(stamp), "Case")
{
}

CaseLogicalFunction::CaseLogicalFunction(CaseLogicalFunction* other) : LogicalFunction(other)
{
    auto otherWhenChildren = getWhenChildren();
    for (auto& whenItr : otherWhenChildren)
    {
        children.push_back(whenItr->clone());
    }
    children.push_back(getDefaultExp()->clone());
}

std::shared_ptr<LogicalFunction>
CaseLogicalFunction::create(const std::vector<std::shared_ptr<LogicalFunction>>& whenExps, const std::shared_ptr<LogicalFunction>& defaultExp)
{
    auto caseNode = std::make_shared<CaseLogicalFunction>(defaultExp->getStamp());
    caseNode->setChildren(whenExps, defaultExp);
    return caseNode;
}

void CaseLogicalFunction::inferStamp(const Schema& schema)
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
            NES::Util::instanceOf<WhenLogicalFunction>(elem),
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
    NES_TRACE("CaseLogicalFunction: we assigned the following stamp: {}", stamp->toString());
}

void CaseLogicalFunction::setChildren(
    std::vector<std::shared_ptr<LogicalFunction>> const& whenExps, std::shared_ptr<LogicalFunction> const& defaultExp)
{
    for (auto elem : whenExps)
    {
        children.push_back(elem);
    }
    children.push_back(defaultExp);
}

std::vector<std::shared_ptr<LogicalFunction>> CaseLogicalFunction::getWhenChildren() const
{
    if (children.size() < 2)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
    std::vector<std::shared_ptr<LogicalFunction>> whenChildren;
    for (auto whenIter = children.begin(); whenIter != children.end() - 1; ++whenIter)
    {
        whenChildren.push_back(Util::as<LogicalFunction>(*whenIter));
    }
    return whenChildren;
}

std::shared_ptr<LogicalFunction> CaseLogicalFunction::getDefaultExp() const
{
    if (children.size() <= 1)
    {
        NES_FATAL_ERROR("A case function always should have at least two children, but it had: {}", children.size());
    }
    return Util::as<LogicalFunction>(*(children.end() - 1));
}

bool CaseLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<CaseLogicalFunction>(rhs))
    {
        auto otherCaseNode = NES::Util::as<CaseLogicalFunction>(rhs);
        if (children.size() != otherCaseNode->children.size())
        {
            return false;
        }
        for (std::size_t i = 0; i < children.size(); i++)
        {
            if (!children.at(i)->equal(otherCaseNode->children.at(i)))
            {
                return false;
            }
        }
        return true;
    }
    return false;
}

std::string CaseLogicalFunction::toString() const
{
    std::stringstream ss;
    ss << "CASE({";
    std::vector<std::shared_ptr<LogicalFunction>> left = getWhenChildren();
    for (std::size_t i = 0; i < left.size() - 1; i++)
    {
        ss << *(left.at(i)) << ",";
    }
    ss << *(*(left.end() - 1)) << "}," << getDefaultExp();

    return ss.str();
}

std::shared_ptr<LogicalFunction> CaseLogicalFunction::clone() const
{
    std::vector<std::shared_ptr<LogicalFunction>> copyOfWhenFunctions;
    for (auto whenFunction : getWhenChildren())
    {
        copyOfWhenFunctions.push_back(NES::Util::as<LogicalFunction>(whenFunction)->clone());
    }
    return CaseLogicalFunction::create(copyOfWhenFunctions, getDefaultExp()->clone());
}

bool CaseLogicalFunction::validateBeforeLowering() const
{
    ///LogicalFunction Currently, we do not have any validation for Case before lowering
    return true;
}

}
