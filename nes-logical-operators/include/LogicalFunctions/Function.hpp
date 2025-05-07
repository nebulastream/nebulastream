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

#include <memory>
#include <ostream>
#include <string>
#include <vector>
#include <API/Schema.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>
#include <SerializableFunction.pb.h>
#include <Common/DataTypes/DataType.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES::Logical
{

struct Function;

/// Concept defining the interface for all logical functions.
///  functions represent operations that can be performed on data
/// during query execution. They are immutable and can have children.
struct FunctionConcept
{
    virtual ~FunctionConcept() = default;
    
    /// Returns a string representation of this function.
    /// @param verbosity The verbosity level for the explanation.
    /// @return std::string The string representation.
    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;

    /// Returns the data type of this function.
    /// @return std::shared_ptr<DataType> The data type.
    [[nodiscard]] virtual std::shared_ptr<DataType> getStamp() const = 0;
    
    /// Creates a new function with the given data type.
    /// @param stamp The new data type.
    /// @return Function The new function.
    [[nodiscard]] virtual Function withStamp(std::shared_ptr<DataType> stamp) const = 0;

    /// Creates a new function with an inferred data type based on the schema.
    /// @param schema The schema to use for inference.
    /// @return Function The new function.
    [[nodiscard]] virtual Function withInferredStamp(const Schema& schema) const = 0;

    /// Returns the children of this function.
    /// @return std::vector<Function> The children.
    [[nodiscard]] virtual std::vector<Function> getChildren() const = 0;
    
    /// Creates a new function with the given children.
    /// @param children The new children.
    /// @return Function The new function.
    [[nodiscard]] virtual Function withChildren(const std::vector<Function>& children) const = 0;

    /// Returns the type of this function.
    /// @return std::string The type.
    [[nodiscard]] virtual std::string_view getType() const = 0;
    
    /// Serializes this function to a protobuf message.
    /// @return SerializableFunction The serialized function.
    [[nodiscard]] virtual SerializableFunction serialize() const = 0;

    /// Compares this function with another function for equality.
    /// @param rhs The function to compare with.
    /// @return bool True if the functions are equal, false otherwise.
    [[nodiscard]] virtual bool operator==(const FunctionConcept& rhs) const = 0;
};

/// @brief Null implementation of the FunctionConcept.
///
/// This class serves as a placeholder or default implementation of a logical function.
/// All operations in this class are undefined and will trigger a runtime precondition failure
/// if invoked. It is intended solely as a safe default for data structures such as hashmaps
/// or error state and should not be used for actual logical function implementations.
class NullFunction : public FunctionConcept
{
public:
    NullFunction();
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;

    [[nodiscard]] std::shared_ptr<DataType> getStamp() const override;
    [[nodiscard]] Function withStamp(std::shared_ptr<DataType>) const override;
    [[nodiscard]] Function withInferredStamp(const Schema& schema) const override;

    [[nodiscard]] std::vector<Function> getChildren() const override;
    [[nodiscard]] Function withChildren(const std::vector<Function>&) const override;

    [[nodiscard]] std::string_view getType() const override;
    [[nodiscard]] SerializableFunction serialize() const override;

    [[nodiscard]] bool operator==(const NullFunction&) const;
    [[nodiscard]] bool operator==(const FunctionConcept&) const override;
};

/// A type-erased wrapper for logical functions.
/// This class provides type erasure for logical functions, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual function implementation.
/// @tparam T The type of the logical function. Must inherit from FunctionConcept.
template<typename T>
concept IsFunction = std::is_base_of_v<FunctionConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Type-erased logical function that can be used in query plans.
struct Function
{
    /// Constructs a Function from a concrete function type.
    /// @tparam T The type of the function. Must satisfy IsFunction concept.
    /// @param op The function to wrap.
    template <IsFunction T>
    Function(const T& op) : self(std::make_unique<Model<T>>(op))
    {
    }

    /// Constructs a NullFunction
    Function();

    Function(const Function& other);
    Function(Function&&) noexcept = default;

    /// Attempts to get the underlying function as type T.
    /// @tparam T The type to try to get the function as.
    /// @return std::optional<T> The function if it is of type T, nullopt otherwise.
    template <typename T>
    [[nodiscard]] std::optional<T> tryGet() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        return std::nullopt;
    }

    /// Gets the underlying function as type T.
    /// @tparam T The type to get the function as.
    /// @return const T The function.
    /// @throw InvalidDynamicCast If the function is not of type T.
    template <typename T>
    [[nodiscard]] const T get() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    Function& operator=(const Function& other);
    
    /// Returns a string representation of this function.
    /// @return std::string The string representation.
    [[nodiscard]] std::string explain(NES::ExplainVerbosity verbosity) const;
    
    /// Returns the data type of this function.
    /// @return std::shared_ptr<DataType> The data type.
    [[nodiscard]] std::shared_ptr<DataType> getStamp() const;
    
    /// Creates a new function with the given data type.
    /// @param stamp The new data type.
    /// @return Function The new function.
    [[nodiscard]] Function withStamp(std::shared_ptr<DataType> stamp) const;
    
    /// Creates a new function with an inferred data type based on the schema.
    /// @param schema The schema to use for inference.
    /// @return Function The new function.
    [[nodiscard]] Function withInferredStamp(const Schema& schema) const;
    
    /// Returns the children of this function.
    /// @return std::vector<Function> The children.
    [[nodiscard]] std::vector<Function> getChildren() const;
    
    /// Creates a new function with the given children.
    /// @param children The new children.
    /// @return Function The new function.
    [[nodiscard]] Function withChildren(const std::vector<Function>& children) const;
    
    /// Returns the type of this function.
    /// @return std::string The type.
    [[nodiscard]] std::string_view getType() const;
    
    /// Serializes this function to a protobuf message.
    /// @return SerializableFunction The serialized function.
    [[nodiscard]] SerializableFunction serialize() const;
    
    /// Compares this function with another function for equality.
    /// @param other The function to compare with.
    /// @return bool True if the functions are equal, false otherwise.
    [[nodiscard]] bool operator==(const Function& other) const;

private:
    struct Concept : FunctionConcept
    {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template <typename T>
    struct Model : Concept
    {
        T data;
        explicit Model(T d) : data(std::move(d)) { }
        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::unique_ptr<Concept>(new Model<T>(data)); }
        [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return data.explain(verbosity); }
        [[nodiscard]] std::vector<Function> getChildren() const override { return data.getChildren(); }
        [[nodiscard]] Function withChildren(const std::vector<Function>& children) const override
        {
            return data.withChildren(children);
        }
        [[nodiscard]] SerializableFunction serialize() const override { return data.serialize(); }
        [[nodiscard]] std::string_view getType() const override { return data.getType(); }
        [[nodiscard]] std::shared_ptr<DataType> getStamp() const override { return data.getStamp(); }
        [[nodiscard]] Function withInferredStamp(const Schema& schema) const override { return data.withInferredStamp(schema); }
        [[nodiscard]] Function withStamp(std::shared_ptr<DataType> stamp) const override { return data.withStamp(stamp); }
        [[nodiscard]] bool operator==(const FunctionConcept& other) const override
        {
            if (auto p = dynamic_cast<const Model<T>*>(&other))
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

inline std::ostream& operator<<(std::ostream& os, const Function& lf)
{
    return os << lf.explain(ExplainVerbosity::Debug);
}

}

namespace std
{
template <>
struct hash<NES::Logical::Function>
{
    std::size_t operator()(const NES::Logical::Function& lf) const noexcept { return std::hash<std::string_view>{}(lf.getType()); }
};
}

FMT_OSTREAM(NES::Logical::Function);
