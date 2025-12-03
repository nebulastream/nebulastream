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
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/DynamicBase.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <nameof.hpp>

namespace NES
{

namespace detail
{
struct ErasedLogicalFunction;
}

template <typename Checked = NES::detail::ErasedLogicalFunction>
struct TypedLogicalFunction;
using LogicalFunction = TypedLogicalFunction<>;

/// Concept defining the interface for all logical functions in the query plan.
/// This concept defines the common interface that all logical functions must implement.
/// Logical functions represent functions in the query plan and are used during query
/// planning and optimization.
template <typename T>
concept LogicalFunctionConcept = requires(
    const T& thisFunction,
    ExplainVerbosity verbosity,
    std::vector<LogicalFunction> children,
    DataType dataType,
    Schema schema,
    const T& rhs) {
    /// Returns a string representation of the function
    { thisFunction.explain(verbosity) } -> std::convertible_to<std::string>;

    /// Returns the children functions of this function
    { thisFunction.getChildren() } -> std::convertible_to<std::vector<LogicalFunction>>;

    /// Returns the data type of the function
    { thisFunction.getDataType() } -> std::convertible_to<DataType>;

    /// Creates a new function with the given children
    { thisFunction.withChildren(children) } -> std::convertible_to<T>;

    /// Creates a new operator with the given datatype
    { thisFunction.withDataType(dataType) } -> std::convertible_to<T>;

    /// Creates a new function with inferred data type based on input schema
    { thisFunction.withInferredDataType(schema) } -> std::same_as<LogicalFunction>;

    /// Returns the type of the function
    { thisFunction.getType() } -> std::convertible_to<std::string_view>;

    /// Serialize the function to a Reflected object
    { NES::reflect(thisFunction) } -> std::same_as<Reflected>;

    /// Compares this function with another for equality
    { thisFunction == rhs } -> std::convertible_to<bool>;
};

namespace detail
{
/// @brief A type erased wrapper for logical functions
struct ErasedLogicalFunction
{
    virtual ~ErasedLogicalFunction() = default;

    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;
    [[nodiscard]] virtual DataType getDataType() const = 0;
    [[nodiscard]] virtual LogicalFunction withDataType(const DataType& dataType) const = 0;
    [[nodiscard]] virtual LogicalFunction withInferredDataType(const Schema& schema) const = 0;
    [[nodiscard]] virtual std::vector<LogicalFunction> getChildren() const = 0;
    [[nodiscard]] virtual LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const = 0;
    [[nodiscard]] virtual std::string_view getType() const = 0;
    [[nodiscard]] virtual Reflected reflect() const = 0;
    [[nodiscard]] virtual bool equals(const ErasedLogicalFunction& other) const = 0;

    friend bool operator==(const ErasedLogicalFunction& lhs, const ErasedLogicalFunction& rhs) { return lhs.equals(rhs); }

private:
    template <typename T>
    friend struct NES::TypedLogicalFunction;
    ///If the function inherits from DynamicBase (over Castable), then returns a pointer to the wrapped operator as DynamicBase,
    ///so that we can then safely try to dyncast the DynamicBase* to Castable<T>*
    [[nodiscard]] virtual std::optional<const DynamicBase*> getImpl() const = 0;
};

template <LogicalFunctionConcept FunctionType>
struct FunctionModel;
}

/// @brief A type erased wrapper for heap-allocated logical functions.
/// @tparam Checked A logical function type that this wrapper guarantees it contains.
/// Either a type erased virtual logical function class or the actual type.
/// You can cast with TypedLogicalFunction::tryGetAs, to try to get a TypedLogicalFunction for a specific type.
template <typename Checked>
struct TypedLogicalFunction
{
    template <LogicalFunctionConcept T>
    requires std::same_as<Checked, NES::detail::ErasedLogicalFunction>
    TypedLogicalFunction(TypedLogicalFunction<T> other) : self(other.self) /// NOLINT(google-explicit-constructor)
    {
    }

    /// Constructs a LogicalFunction from a concrete function type.
    /// @tparam T The type of the function. Must satisfy LogicalFunctionConcept concept.
    /// @param op The function to wrap.
    template <typename T>
    requires requires(const T& function) {
        { function.getChildren() } -> std::convertible_to<std::vector<LogicalFunction>>;
    }
    TypedLogicalFunction(const T& op) : self(std::make_shared<NES::detail::FunctionModel<T>>(op)) /// NOLINT(google-explicit-constructor)
    {
    }

    template <LogicalFunctionConcept T>
    TypedLogicalFunction(const NES::detail::FunctionModel<T>& op) /// NOLINT(google-explicit-constructor)
        : self(std::make_shared<NES::detail::FunctionModel<T>>(op.impl))
    {
    }

    explicit TypedLogicalFunction(std::shared_ptr<const NES::detail::ErasedLogicalFunction> op) : self(std::move(op)) { }

    TypedLogicalFunction() = default;

    ///@brief Alternative to operator*
    [[nodiscard]] const Checked& get() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedLogicalFunction, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedLogicalFunction>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::FunctionModel<Checked>>(self)->impl;
        }
    }

    const Checked& operator*() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedLogicalFunction, Checked>)
        {
            return *std::dynamic_pointer_cast<const NES::detail::ErasedLogicalFunction>(self);
        }
        else
        {
            return std::dynamic_pointer_cast<const NES::detail::FunctionModel<Checked>>(self)->impl;
        }
    }

    const Checked* operator->() const
    {
        if constexpr (std::is_same_v<NES::detail::ErasedLogicalFunction, Checked>)
        {
            return std::dynamic_pointer_cast<const NES::detail::ErasedLogicalFunction>(self).get();
        }
        else
        {
            auto casted = std::dynamic_pointer_cast<const NES::detail::FunctionModel<Checked>>(self);
            return &casted->impl;
        }
    }

    /// Attempts to get the underlying function as type T.
    /// @tparam T The type to try to get the function as.
    /// @return std::optional<TypedLogicalFunction<T>> The function if it is of type T, nullopt otherwise.
    template <LogicalFunctionConcept T>
    std::optional<TypedLogicalFunction<T>> tryGetAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::FunctionModel<T>>(self))
        {
            return TypedLogicalFunction<T>{std::static_pointer_cast<const NES::detail::ErasedLogicalFunction>(model)};
        }
        return std::nullopt;
    }

    /// Attempts to get the underlying function as type T.
    /// @tparam T The type to try to get the function as.
    /// @return std::optional<std::shared_ptr<const Castable<T>>> The function if it is of type T and Castable<T>, nullopt otherwise.
    template <typename T>
    requires(!LogicalFunctionConcept<T>)
    std::optional<std::shared_ptr<const Castable<T>>> tryGetAs() const
    {
        if (auto castable = self->getImpl(); castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const Castable<T>>{self, ptr};
            }
        }
        return std::nullopt;
    }

    /// Gets the underlying function as type T.
    /// @tparam T The type to get the function as.
    /// @return TypedLogicalFunction<T> The function.
    /// @throw InvalidDynamicCast If the function is not of type T.
    template <LogicalFunctionConcept T>
    TypedLogicalFunction<T> getAs() const
    {
        if (auto model = std::dynamic_pointer_cast<const NES::detail::FunctionModel<T>>(self))
        {
            return TypedLogicalFunction<T>{std::static_pointer_cast<const NES::detail::ErasedLogicalFunction>(model)};
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
        std::unreachable();
    }

    /// Gets the underlying function as type T.
    /// @tparam T The type to get the function as.
    /// @return std::shared_ptr<const Castable<T>> The function.
    /// @throw InvalidDynamicCast If the function is not of type T or does not inherit from Castable<T>.
    template <typename T>
    requires(!LogicalFunctionConcept<T>)
    std::shared_ptr<const Castable<T>> getAs() const
    {
        if (auto castable = self->getImpl(); castable.has_value())
        {
            if (auto ptr = dynamic_cast<const Castable<T>*>(castable.value()))
            {
                return std::shared_ptr<const Castable<T>>{self, ptr};
            }
        }
        PRECONDITION(false, "requested type {} , but stored type is {}", NAMEOF_TYPE(T), NAMEOF_TYPE_EXPR(self));
        std::unreachable();
    }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const { return self->explain(verbosity); };

    [[nodiscard]] DataType getDataType() const { return self->getDataType(); };

    [[nodiscard]] LogicalFunction withDataType(const DataType& dataType) const { return self->withDataType(dataType); };

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const { return self->withInferredDataType(schema); };

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const { return self->getChildren(); };

    [[nodiscard]] TypedLogicalFunction withChildren(const std::vector<LogicalFunction>& children) const
    {
        return self->withChildren(children);
    };

    [[nodiscard]] std::string_view getType() const { return self->getType(); };

    [[nodiscard]] bool operator==(const LogicalFunction& other) const { return self->equals(*other.self); };

private:
    template <typename FriendChecked>
    friend struct TypedLogicalFunction;

    std::shared_ptr<const NES::detail::ErasedLogicalFunction> self;
};

namespace detail
{
/// @brief Wrapper type that acts as a bridge between a type satisfying LogicalFunctionConcept and TypedLogicalFunction
template <LogicalFunctionConcept FunctionType>
struct FunctionModel : ErasedLogicalFunction
{
    FunctionType impl;

    explicit FunctionModel(FunctionType impl) : impl(std::move(impl)) { }

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return impl.explain(verbosity); }

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const override { return impl.getChildren(); }

    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override
    {
        return impl.withChildren(children);
    }

    [[nodiscard]] Reflected reflect() const override { return NES::reflect(impl); }

    [[nodiscard]] std::string_view getType() const override { return impl.getType(); }

    [[nodiscard]] DataType getDataType() const override { return impl.getDataType(); }

    [[nodiscard]] FunctionType get() const { return impl; }

    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override { return impl.withInferredDataType(schema); }

    [[nodiscard]] LogicalFunction withDataType(const DataType& dataType) const override { return impl.withDataType(dataType); }

    [[nodiscard]] bool operator==(const LogicalFunction& other) const
    {
        if (auto ptr = dynamic_cast<const FunctionModel*>(&other))
        {
            return impl.operator==(ptr->impl);
        }
        return false;
    }

    [[nodiscard]] bool equals(const ErasedLogicalFunction& other) const override
    {
        if (auto ptr = dynamic_cast<const FunctionModel*>(&other))
        {
            return impl.operator==(ptr->impl);
        }
        return false;
    }

private:
    template <typename T>
    friend struct TypedLogicalFunction;

    [[nodiscard]] std::optional<const DynamicBase*> getImpl() const override
    {
        if constexpr (std::is_base_of_v<DynamicBase, FunctionType>)
        {
            return static_cast<const DynamicBase*>(&impl);
        }
        else
        {
            return std::nullopt;
        }
    }
};
}

inline std::ostream& operator<<(std::ostream& os, const LogicalFunction& lf)
{
    return os << lf.explain(ExplainVerbosity::Debug);
}

}

namespace std
{
template <>
struct hash<NES::LogicalFunction>
{
    std::size_t operator()(const NES::LogicalFunction& lf) const noexcept { return std::hash<std::string_view>{}(lf.getType()); }
};
}

FMT_OSTREAM(NES::LogicalFunction);

namespace NES
{
class FieldIdentifier
{
    std::string fieldName;

public:
    explicit FieldIdentifier(std::string fieldName) : fieldName(std::move(fieldName)) { }

    [[nodiscard]] const std::string& getFieldName() const { return fieldName; }

    auto operator<=>(const FieldIdentifier&) const = default;
};
}

namespace std
{
template <>
struct hash<NES::FieldIdentifier>
{
    std::size_t operator()(const NES::FieldIdentifier& identifier) const noexcept
    {
        return std::hash<std::string>{}(identifier.getFieldName());
    }
};
}
