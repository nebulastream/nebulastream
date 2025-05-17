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

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <API/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <Common/DataTypes/DataType.hpp>

namespace NES
{

struct LogicalFunction;

/// Concept defining the interface for all logical functions.
/// Logical functions represent operations that can be performed on data
/// during query execution. They are immutable and can have children.
struct LogicalFunctionConcept
{
    virtual ~LogicalFunctionConcept() = default;
    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;

    [[nodiscard]] virtual std::shared_ptr<DataType> getDataType() const = 0;
    [[nodiscard]] virtual LogicalFunction withDataType(std::shared_ptr<DataType> dataType) const = 0;
    [[nodiscard]] virtual LogicalFunction withInferredDataType(const Schema& schema) const = 0;

    [[nodiscard]] virtual std::vector<LogicalFunction> getChildren() const = 0;
    [[nodiscard]] virtual LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const = 0;

    [[nodiscard]] virtual std::string_view getType() const = 0;
    [[nodiscard]] virtual SerializableFunction serialize() const = 0;

    [[nodiscard]] virtual bool operator==(const LogicalFunctionConcept& rhs) const = 0;
};

/// A type-erased wrapper for logical functions.
/// C.f.: https://sean-parent.stlab.cc/presentations/2017-01-18-runtime-polymorphism/2017-01-18-runtime-polymorphism.pdf
/// This class provides type erasure for logical functions, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual function implementation.
/// @tparam T The type of the logical function. Must inherit from LogicalFunctionConcept.
template <typename T>
concept IsLogicalFunction = std::is_base_of_v<LogicalFunctionConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Type-erased logical function that can be used in query plans.
struct LogicalFunction
{
    /// Constructs a LogicalFunction from a concrete function type.
    /// @tparam T The type of the function. Must satisfy IsLogicalFunction concept.
    /// @param op The function to wrap.
    template <IsLogicalFunction T>
    LogicalFunction(const T& op) : self(std::make_unique<Model<T>>(op)) /// NOLINT
    {
    }

    /// Constructs a NullLogicalFunction
    LogicalFunction();

    LogicalFunction(const LogicalFunction& other);
    LogicalFunction(LogicalFunction&&) noexcept = default;

    /// Attempts to get the underlying function as type T.
    /// @tparam T The type to try to get the function as.
    /// @return std::optional<T> The function if it is of type T, nullopt otherwise.
    template <typename T>
    [[nodiscard]] std::optional<T> tryGet() const
    {
        if (auto model = dynamic_cast<const Model<T>*>(self.get()))
        {
            return model->data;
        }
        return std::nullopt;
    }

    /// Gets the underlying function as type T.
    /// @tparam T The type to get the function as.
    /// @return const T The function.
    /// @throw InvalidDynamicCast If the function is not of type T.
    template <typename T>
    [[nodiscard]] T get() const
    {
        if (auto model = dynamic_cast<const Model<T>*>(self.get()))
        {
            return model->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    LogicalFunction& operator=(const LogicalFunction& other);

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] std::shared_ptr<DataType> getDataType() const;
    [[nodiscard]] LogicalFunction withDataType(std::shared_ptr<DataType> dataType) const;
    [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const;

    [[nodiscard]] std::vector<LogicalFunction> getChildren() const;
    [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const;

    [[nodiscard]] std::string_view getType() const;
    [[nodiscard]] SerializableFunction serialize() const;
    [[nodiscard]] bool operator==(const LogicalFunction& other) const;

private:
    struct Concept : LogicalFunctionConcept
    {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template <IsLogicalFunction FunctionType>
    struct Model : Concept
    {
        FunctionType data;
        explicit Model(FunctionType d) : data(std::move(d)) { }
        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::unique_ptr<Concept>(new Model(data)); }
        [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return data.explain(verbosity); }
        [[nodiscard]] std::vector<LogicalFunction> getChildren() const override { return data.getChildren(); }
        [[nodiscard]] LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override
        {
            return data.withChildren(children);
        }
        [[nodiscard]] SerializableFunction serialize() const override { return data.serialize(); }
        [[nodiscard]] std::string_view getType() const override { return data.getType(); }
        [[nodiscard]] std::shared_ptr<DataType> getDataType() const override { return data.getDataType(); }
        [[nodiscard]] LogicalFunction withInferredDataType(const Schema& schema) const override
        {
            return data.withInferredDataType(schema);
        }
        [[nodiscard]] LogicalFunction withDataType(std::shared_ptr<DataType> dataType) const override
        {
            return data.withDataType(dataType);
        }
        [[nodiscard]] bool operator==(const LogicalFunctionConcept& other) const override
        {
            if (auto p = dynamic_cast<const Model*>(&other))
            {
                return data.operator==(p->data);
            }
            return false;
        }
        [[nodiscard]] bool equals(const Concept& other) const override
        {
            if (auto p = dynamic_cast<const Model*>(&other))
            {
                return data.operator==(p->data);
            }
            return false;
        }
    };

    std::unique_ptr<Concept> self;
};

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
