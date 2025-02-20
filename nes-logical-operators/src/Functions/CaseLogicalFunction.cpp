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
#include <Serialization/DataTypeSerializationUtil.hpp>

namespace NES
{
CaseLogicalFunction::~CaseLogicalFunction() noexcept = default;

CaseLogicalFunction::CaseLogicalFunction(const CaseLogicalFunction& other) : LogicalFunction(other)
{
    for (const auto& whenChild : other.whenChildren) {
        whenChildren.push_back(whenChild->clone());
    }
    defaultChild = other.defaultChild->clone();
    allChildren = whenChildren;
    allChildren.push_back(defaultChild);
}

CaseLogicalFunction::CaseLogicalFunction(const std::vector<std::shared_ptr<LogicalFunction>>& whenExps, const std::shared_ptr<LogicalFunction>& defaultExp)
    : LogicalFunction(defaultExp->getStamp())
{
    setChildren(whenExps, defaultExp);
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
    whenChildren = whenExps;
    defaultChild = defaultExp;

    // Update the combined container.
    allChildren = whenChildren;
    allChildren.push_back(defaultChild);
}

std::vector<std::shared_ptr<LogicalFunction>> CaseLogicalFunction::getWhenChildren() const
{
    return whenChildren;
}

std::shared_ptr<LogicalFunction> CaseLogicalFunction::getDefaultExp() const
{
    return defaultChild;
}

bool CaseLogicalFunction::operator==(std::shared_ptr<LogicalFunction> const& rhs) const
{
    if (NES::Util::instanceOf<CaseLogicalFunction>(rhs))
    {
        auto otherCaseNode = NES::Util::as<CaseLogicalFunction>(rhs);
        if (defaultChild != otherCaseNode->defaultChild)
        {
            return false;
        }
        if (whenChildren.size() != otherCaseNode->whenChildren.size())
        {
            return false;
        }
        for (std::size_t i = 0; i < whenChildren.size(); i++)
        {
            if (whenChildren.at(i) != otherCaseNode->whenChildren.at(i))
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
    return std::make_shared<CaseLogicalFunction>(copyOfWhenFunctions, getDefaultExp()->clone());
}

bool CaseLogicalFunction::validateBeforeLowering() const
{
    ///LogicalFunction Currently, we do not have any validation for Case before lowering
    return true;
}

std::span<const std::shared_ptr<LogicalFunction>> CaseLogicalFunction::getChildren() const
{
    return { allChildren.data(), allChildren.size() };
}

SerializableFunction CaseLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_functiontype(NAME);

    auto* funcDesc = new SerializableFunction_NaryFunction();

    NES::FunctionList list;
    auto* defaultFunc = list.add_functions();
    defaultFunc->CopyFrom(this->defaultChild->serialize());
    for (const auto& function : this->whenChildren)
    {
        auto* whenFunc = list.add_functions();
        whenFunc->CopyFrom(function->serialize());
    }

    DataTypeSerializationUtil::serializeDataType(
        this->getStamp(), serializedFunction.mutable_stamp());

    return serializedFunction;
}

}
