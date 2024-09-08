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
#include <Functions/Functions/FunctionFunctionNode.hpp>
#include <Functions/Functions/LogicalFunctionRegistry.hpp>
#include <Util/Common.hpp>
#include <fmt/format.h>

namespace NES
{

FunctionFunction::FunctionFunction(DataTypePtr stamp, std::string functionName, std::unique_ptr<LogicalFunction> function)
    : FunctionNode(std::move(stamp)), functionName(std::move(functionName)), function(std::move(function))
{
}

FunctionNodePtr
FunctionFunction::create(const DataTypePtr& stamp, const std::string& functionName, const std::vector<FunctionNodePtr>& arguments)
{
    if (auto function = LogicalFunctionRegistry::instance().create(functionName))
    {
        throw UnknownExpressionType(fmt::format("Exception of type: {} not registered.", functionName));
    }
    else
    {
        auto functionNode = std::make_shared<FunctionFunction>(stamp, functionName, std::move(function.value()));
        for (const auto& arg : arguments)
        {
            functionNode->addChild(arg);
        }
        return functionNode;
    }
}

void FunctionFunction::inferStamp(SchemaPtr schema)
{
    std::vector<DataTypePtr> argumentTypes;
    for (const auto& input : getArguments())
    {
        input->inferStamp(schema);
        argumentTypes.emplace_back(input->getStamp());
    }
    auto resultStamp = function->inferStamp(argumentTypes);
    setStamp(resultStamp);
}

std::string FunctionFunction::toString() const
{
    return functionName;
}

bool FunctionFunction::equal(const NodePtr& rhs) const
{
    if (NES::Util::instanceOf<FunctionFunction>(rhs))
    {
        auto otherAddNode = NES::Util::as<FunctionFunction>(rhs);
        return functionName == otherAddNode->functionName;
    }
    return false;
}

FunctionNodePtr FunctionFunction::copy()
{
    return FunctionFunction::create(stamp, functionName, getArguments());
}
const std::string& FunctionFunction::getFunctionName() const
{
    return functionName;
}
std::vector<FunctionNodePtr> FunctionFunction::getArguments() const
{
    std::vector<FunctionNodePtr> arguments;
    for (const auto& child : getChildren())
    {
        arguments.emplace_back(NES::Util::as<FunctionNode>(child));
    }
    return arguments;
}

} /// namespace NES
