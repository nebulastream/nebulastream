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

#pragma once

/// @file FunctionPluginMacros.hpp
/// @brief Single-line macros to define complete logical function plugins.
///
/// ## Typed functions (most common — no body needed)
///
///     /// Unary: input → output
///     NES_LOGICAL_FUNCTION(CastFromUnixTs, UINT64, VARSIZED)
///
///     /// Binary: (input1, input2) → output
///     NES_LOGICAL_FUNCTION(Uuid, UINT64, UINT64, VARSIZED)
///
///     /// Any arity works:
///     NES_LOGICAL_FUNCTION(MyFunc, INT32, INT32, FLOAT64, BOOLEAN, VARSIZED)
///
/// ## Custom type inference (for functions where output type depends on input types)
///
///     NES_LOGICAL_FUNCTION_CUSTOM(2, Add)
///     {
///         /// `children` contains the already-inferred child functions.
///         /// Return the output DataType::Type.
///         auto joined = children[0].getDataType().join(children[1].getDataType());
///         if (!joined) throw ...;
///         return joined->type;
///     }
///
/// Both macros must be invoked inside `namespace NES { }`.

#include <initializer_list>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES::detail
{

/// Validates input types and returns the output type. Last pack element is the output type,
/// all preceding are expected input types. Arity = sizeof...(Types) - 1.
template <DataType::Type... Types>
DataType::Type validateFixedTypes(std::string_view funcName, const std::vector<LogicalFunction>& children)
{
    constexpr std::array types{Types...};
    constexpr size_t arity = sizeof...(Types) - 1;
    static_assert(arity >= 1, "Need at least one input type and one output type");
    for (size_t i = 0; i < arity; ++i)
    {
        if (children[i].getDataType().type != DataType::Type::UNDEFINED && children[i].getDataType().type != types[i])
        {
            throw DifferentFieldTypeExpected("{}: argument {} has unexpected type {}", funcName, i, children[i].getDataType());
        }
    }
    return types[arity];
}

/// Formats a function call as "NAME(child1, child2, ...)".
inline std::string explainFunction(std::string_view name, const std::vector<LogicalFunction>& children, ExplainVerbosity verbosity)
{
    std::string result(name);
    result += '(';
    for (size_t i = 0; i < children.size(); ++i)
    {
        if (i > 0)
        {
            result += ", ";
        }
        result += children[i].explain(verbosity);
    }
    result += ')';
    return result;
}

}

/// NOLINTBEGIN(cppcoreguidelines-macro-usage)

#define NES_PP_CAT(a, b) a##b
#define NES_PP_XCAT(a, b) NES_PP_CAT(a, b)
#define NES_PP_NARGS_IMPL(_1, _2, _3, _4, _5, _6, _7, _8, N, ...) N
#define NES_PP_NARGS(...) NES_PP_NARGS_IMPL(__VA_ARGS__, 8, 7, 6, 5, 4, 3, 2, 1)
#define NES_PP_DEC_2 1
#define NES_PP_DEC_3 2
#define NES_PP_DEC_4 3
#define NES_PP_DEC_5 4
#define NES_PP_DEC_6 5
#define NES_PP_DEC_7 6
#define NES_PP_DEC_8 7
#define NES_PP_DEC(n) NES_PP_XCAT(NES_PP_DEC_, n)

/// Fully auto-generated typed function. Arity inferred, types validated, errors auto-generated.
/// NES_LOGICAL_FUNCTION(Name, InputType1, [InputType2, ...], OutputType)
/// Type names are bare enum values (UINT64, VARSIZED, etc.) — resolved via `using enum DataType::Type`.
#define NES_LOGICAL_FUNCTION(name, ...) \
    NES_LOGICAL_FUNCTION_IMPL(name, NES_PP_DEC(NES_PP_NARGS(__VA_ARGS__))) \
    { \
        using enum DataType::Type; \
        return detail::validateFixedTypes<__VA_ARGS__>(#name, children); \
    }

/// Custom type inference. Provide a body that receives `children` and returns `DataType::Type`.
/// NES_LOGICAL_FUNCTION_CUSTOM(arity, Name) { ... return DataType::Type::FOO; }
#define NES_LOGICAL_FUNCTION_CUSTOM(arity, name) NES_LOGICAL_FUNCTION_IMPL(name, arity)

#define NES_LOGICAL_FUNCTION_IMPL(name, arity) \
    class name##LogicalFunction final \
    { \
    public: \
        static constexpr std::string_view NAME = #name; \
        static constexpr size_t ARITY = (arity); \
        explicit name##LogicalFunction(std::vector<LogicalFunction> children); \
        [[nodiscard]] bool operator==(const name##LogicalFunction& rhs) const; \
        [[nodiscard]] DataType getDataType() const; \
        [[nodiscard]] name##LogicalFunction withDataType(const DataType& dataType) const; \
        [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const; \
        [[nodiscard]] std::vector<LogicalFunction> getChildren() const; \
        [[nodiscard]] name##LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const; \
        [[nodiscard]] std::string_view getType() const; \
        [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const; \
\
    private: \
        [[nodiscard]] DataType::Type inferOutputType_(const std::vector<LogicalFunction>& children) const; \
        DataType dataType; \
        std::vector<LogicalFunction> children_; \
        friend Reflector<name##LogicalFunction>; \
    }; \
    static_assert(LogicalFunctionConcept<name##LogicalFunction>); \
    template <> \
    struct Reflector<name##LogicalFunction> \
    { \
        Reflected operator()(const name##LogicalFunction& function) const; \
    }; \
    template <> \
    struct Unreflector<name##LogicalFunction> \
    { \
        name##LogicalFunction operator()(const Reflected& reflected) const; \
    }; \
    } \
    namespace NES::detail \
    { \
    struct Reflected##name##LogicalFunction \
    { \
        std::vector<LogicalFunction> children; \
    }; \
    } \
    FMT_OSTREAM(NES::name##LogicalFunction); \
    namespace NES \
    { \
    name##LogicalFunction::name##LogicalFunction(std::vector<LogicalFunction> children) \
        : dataType(DataTypeProvider::provideDataType(DataType::Type::UNDEFINED)), children_(std::move(children)) \
    { \
    } \
    bool name##LogicalFunction::operator==(const name##LogicalFunction & rhs) const \
    { \
        return children_ == rhs.children_; \
    } \
    DataType name##LogicalFunction::getDataType() const \
    { \
        return dataType; \
    } \
    name##LogicalFunction name##LogicalFunction::withDataType(const DataType& dataType) const \
    { \
        auto copy = *this; \
        copy.dataType = dataType; \
        return copy; \
    } \
    std::string_view name##LogicalFunction::getType() const \
    { \
        return NAME; \
    } \
    std::vector<LogicalFunction> name##LogicalFunction::getChildren() const \
    { \
        return children_; \
    } \
    name##LogicalFunction name##LogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const \
    { \
        PRECONDITION(children.size() == ARITY, #name "LogicalFunction requires {} children, but got {}", ARITY, children.size()); \
        auto copy = *this; \
        copy.children_ = children; \
        return copy; \
    } \
    std::string name##LogicalFunction::explain(ExplainVerbosity verbosity) const \
    { \
        return detail::explainFunction(NAME, children_, verbosity); \
    } \
    LogicalFunction name##LogicalFunction::withInferredDataType(const Schema& schema) const \
    { \
        const auto newChildren = children_ \
            | std::views::transform([&schema](const auto& child) { return child.withInferredDataType(schema); }) \
            | std::ranges::to<std::vector>(); \
        INVARIANT(newChildren.size() == ARITY, #name "LogicalFunction expects {} children but has {}", ARITY, newChildren.size()); \
        auto newDataType = DataTypeProvider::provideDataType(inferOutputType_(newChildren)); \
        newDataType.nullable = std::ranges::any_of(newChildren, [](const auto& c) { return c.getDataType().nullable; }); \
        return withDataType(newDataType).withChildren(newChildren); \
    } \
    Reflected Reflector<name##LogicalFunction>::operator()(const name##LogicalFunction& function) const \
    { \
        return reflect(detail::Reflected##name##LogicalFunction{.children = function.children_}); \
    } \
    name##LogicalFunction Unreflector<name##LogicalFunction>::operator()(const Reflected& reflected) const \
    { \
        auto result = unreflect<detail::Reflected##name##LogicalFunction>(reflected); \
        return name##LogicalFunction{std::move(result.children)}; \
    } \
    LogicalFunctionRegistryReturnType \
        LogicalFunctionGeneratedRegistrar::Register##name##LogicalFunction(LogicalFunctionRegistryArguments arguments) \
    { \
        if (!arguments.reflected.isEmpty()) \
        { \
            return unreflect<name##LogicalFunction>(arguments.reflected); \
        } \
        if (arguments.children.size() != (arity)) \
        { \
            throw CannotDeserialize(#name "LogicalFunction requires " #arity " children, but got {}", arguments.children.size()); \
        } \
        return name##LogicalFunction(std::move(arguments.children)); \
    } \
    DataType::Type name##LogicalFunction::inferOutputType_([[maybe_unused]] const std::vector<LogicalFunction>& children) const

/// NOLINTEND(cppcoreguidelines-macro-usage)
