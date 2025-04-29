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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

namespace NES::Functions
{
using namespace Nautilus;

/// Concept defining the interface for all physical functions.
struct PhysicalFunctionConcept
{
    virtual ~PhysicalFunctionConcept() = default;
    
    /// Executes the function on the given record.
    /// @param record The record to evaluate the function on.
    /// @param arena The arena to allocate memory from.
    /// @return The result of the function evaluation.
    [[nodiscard]] virtual VarVal execute(const Record& record, ArenaRef& arena) const = 0;
};

/// A type-erased wrapper for physical functions.
/// This class provides type erasure for physical functions, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual function implementation.
/// @tparam T The type of the physical function. Must inherit from PhysicalFunctionConcept.
template<typename T>
concept IsPhysicalFunction = std::is_base_of_v<PhysicalFunctionConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Type-erased physical function that can be used to evaluate expressions on records.
struct PhysicalFunction
{
    /// Constructs a PhysicalFunction from a concrete function type.
    /// @tparam T The type of the function. Must satisfy IsPhysicalFunction concept.
    /// @param fn The function to wrap.
    template <IsPhysicalFunction T>
    PhysicalFunction(const T& fn) : self(std::make_unique<Model<T>>(fn))
    {
    }

    /// Executes the function on the given record.
    /// @param record The record to evaluate the function on.
    /// @param arena The arena to allocate memory from.
    /// @return The result of the function evaluation.
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const
    {
        return self->execute(record, arena);
    }

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

    PhysicalFunction(PhysicalFunction&&) noexcept = default;

    PhysicalFunction(const PhysicalFunction& other) : self(other.self->clone()) { }

    PhysicalFunction& operator=(const PhysicalFunction& other)
    {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

private:
    struct Concept : PhysicalFunctionConcept
    {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
    };

    template <typename T>
    struct Model : Concept
    {
        T data;

        explicit Model(T d) : data(std::move(d)) { }

        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::unique_ptr<Concept>(new Model(data)); }

        [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override
        {
            return data.execute(record, arena);
        }
    };

    std::unique_ptr<Concept> self;
};
}
