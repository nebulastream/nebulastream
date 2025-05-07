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

#include "NodeFunctionImageManip.hpp"
#include <concepts>
#include <ranges>
#include <Functions/NodeFunctionConstantValue.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Ranges.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <fmt/std.h>
#include <Common/DataTypes/Integer.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>

#include "LogicalFunctionRegistry.hpp"

NES::NodeFunctionImageManip::NodeFunctionImageManip(std::shared_ptr<DataType> stamp, std::string type)
    : NodeFunction(std::move(stamp), std::move(type))
{
}


struct ImageManipFunction
{
    virtual ~ImageManipFunction() = default;
    virtual std::shared_ptr<NES::DataType> initialStamp() = 0;
    virtual void check(std::span<const std::shared_ptr<NES::Node>> args) = 0;
};
template <std::derived_from<NES::DataType> InitialStamp, typename... Args>
struct ImageManipFunctionImpl : ImageManipFunction
{
    explicit ImageManipFunctionImpl(std::string name) : name(std::move(name)) { }
    std::string name;
    std::shared_ptr<NES::DataType> initialStamp() override { return std::make_shared<InitialStamp>(); }

    void check(std::span<const std::shared_ptr<NES::Node>> args) override
    {
        if (args.size() - 1 != sizeof...(Args)) /// Skip the first argument as it is the function name
        {
            throw NES::CannotInferSchema(
                "Error during stamp inference. ImageManip{} requires {} parameter, but received: {}",
                name,
                sizeof...(Args),
                args.size() - 1);
        }

        auto types = args | std::views::drop(1)
            | std::views::transform([](const auto& node) { return NES::Util::as<NES::NodeFunction>(node)->getStamp(); })
            | std::ranges::to<std::vector>();

        auto it = std::begin(types);
        if (!(NES::Util::instanceOf<Args>(*it++) && ...))
        {
            throw NES::CannotInferSchema(
                "Error during stamp inference. ImageManip{} expects parameter(s): {}\nBut received: {}",
                name,
                ((std::string(typeid(Args).name()) + std::string(",")) + ...),
                fmt::join(std::views::transform(types, [](const auto& type) { return type->toString(); }), ", "));
        }
    }
};

static std::unordered_map<std::string, std::shared_ptr<ImageManipFunction>> functions
    = {{"FromBase64", std::make_shared<ImageManipFunctionImpl<NES::VariableSizedDataType, NES::VariableSizedDataType>>("FromBase64")},
       {"ToBase64", std::make_shared<ImageManipFunctionImpl<NES::VariableSizedDataType, NES::VariableSizedDataType>>("ToBase64")},
       {"Mono16ToGray", std::make_shared<ImageManipFunctionImpl<NES::VariableSizedDataType, NES::VariableSizedDataType>>("Mono16ToGray")},
       {"DrawRectangle",
        std::make_shared<ImageManipFunctionImpl<
            NES::VariableSizedDataType,
            NES::VariableSizedDataType,
            NES::Integer,
            NES::Integer,
            NES::Integer,
            NES::Integer>>("DrawRectangle")}

};


std::shared_ptr<NES::NodeFunction> NES::NodeFunctionImageManip::create(std::vector<std::shared_ptr<NodeFunction>> children)
{
    PRECONDITION(
        children.size() > 0 && Util::instanceOf<NodeFunctionConstantValue>(children.at(0))
            && Util::instanceOf<VariableSizedDataType>(Util::as<NodeFunctionConstantValue>(children.at(0))->getStamp()),
        "ImageManip requires at least the function name");

    auto functionName = Util::as_if<NodeFunctionConstantValue>(children[0])->getConstantValue();
    auto function = functions.find(functionName);
    if (function == functions.end())
    {
        throw FunctionNotImplemented(fmt::format("ImageManip{} does not exist", functionName));
    }

    auto imageManipNode
        = std::make_shared<NodeFunctionImageManip>(function->second->initialStamp(), fmt::format("ImageManip{}", functionName));

    imageManipNode->functionName = functionName;
    imageManipNode->children
        = std::views::transform(
              std::move(children),
              [](std::shared_ptr<NodeFunction> arg) -> std::shared_ptr<Node> { return std::dynamic_pointer_cast<Node>(arg); })
        | std::ranges::to<std::vector>();
    return imageManipNode;
}

bool NES::NodeFunctionImageManip::equal(const std::shared_ptr<Node>& rhs) const
{
    return Util::instanceOf<NodeFunctionImageManip>(rhs) && Util::as<NodeFunctionImageManip>(rhs)->functionName == this->functionName
        && std::ranges::equal(children, rhs->getChildren(), [](auto& a, auto& b) { return a->equal(b); });
}

void NES::NodeFunctionImageManip::inferStamp(const Schema& schema)
{
    for (auto& child : children)
    {
        Util::as<NodeFunction>(child)->inferStamp(schema);
    }

    functions.at(functionName)->check(children);
}

std::shared_ptr<NES::NodeFunction> NES::NodeFunctionImageManip::deepCopy()
{
    return NodeFunctionImageManip::create(
        std::views::transform(children, [](const auto& child) { return Util::as<NodeFunction>(child)->deepCopy(); })
        | std::ranges::to<std::vector>());
}

bool NES::NodeFunctionImageManip::validateBeforeLowering() const
{
    try
    {
        functions.at(functionName)->check(children);
        return true;
    }
    catch (const Exception& e)
    {
        NES_ERROR("{}", e.what());
    }
    return false;
}
std::ostream& NES::NodeFunctionImageManip::toQueryPlanString(std::ostream& os) const
{
    fmt::print(
        os,
        "ImageManip{}({:q})",
        this->functionName,
        fmt::join(this->children | std::views::drop(1) | std::views::transform([](const auto& child) -> Node& { return *child; }), ", "));
    return os;
}
std::ostream& NES::NodeFunctionImageManip::toDebugString(std::ostream& os) const
{
    fmt::print(
        os,
        "ImageManip{}({})",
        this->functionName,
        fmt::join(this->children | std::views::drop(1) | std::views::transform([](const auto& child) -> Node& { return *child; }), ", "));
    return os;
}


const std::string& NES::NodeFunctionImageManip::getFunctionName() const
{
    return functionName;
}

namespace NES::LogicalFunctionGeneratedRegistrar
{
/// declaration of register functions for 'LogicalFunctions'
LogicalFunctionRegistryReturnType RegisterImageManipLogicalFunction(LogicalFunctionRegistryArguments arguments)
{
    auto function = NodeFunctionImageManip::create(arguments.childFunctions);
    return function;
}
}
