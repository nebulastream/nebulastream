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
#include <optional>
#include <type_traits>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>

namespace NES
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
/// C.f.: https://sean-parent.stlab.cc/presentations/2017-01-18-runtime-polymorphism/2017-01-18-runtime-polymorphism.pdf
/// This class provides type erasure for physical functions, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual function implementation.
/// @tparam T The type of the physical function. Must inherit from PhysicalFunctionConcept.
template <typename T>
concept IsPhysicalFunction = std::is_base_of_v<PhysicalFunctionConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Type-erased physical function that can be used to evaluate expressions on records.
struct PhysicalFunction
{
    /// Constructs a PhysicalFunction from a concrete function type.
    /// @tparam FunctionType The type of the function. Must satisfy IsPhysicalFunction concept.
    /// @param fn The function to wrap.
    template <IsPhysicalFunction FunctionType>
    PhysicalFunction(const FunctionType& fn) /// NOLINT
        : self(std::make_shared<Model<FunctionType>>(fn))
    {
    }

    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const { return self->execute(record, arena); }

    /// Attempts to get the underlying function as type FunctionType.
    /// @tparam FunctionType The type to try to get the function as.
    /// @return std::optional<FunctionType> The function if it is of type FunctionType, nullopt otherwise.
    template <IsPhysicalFunction FunctionType>
    [[nodiscard]] std::optional<FunctionType> tryGet() const
    {
        if (auto p = dynamic_cast<const Model<FunctionType>*>(self.get()))
        {
            return p->data;
        }
        return std::nullopt;
    }

    /// Gets the underlying function as type T.
    /// @tparam FunctionType The type to get the function as.
    /// @return const FunctionType The function.
    /// @throw InvalidDynamicCast If the function is not of type FunctionType.
    template <typename FunctionType>
    [[nodiscard]] FunctionType get() const
    {
        if (auto p = dynamic_cast<const Model<FunctionType>*>(self.get()))
        {
            return p->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(FunctionType).name(), typeid(self).name());
    }

    PhysicalFunction(PhysicalFunction&&) noexcept = default;

    PhysicalFunction(const PhysicalFunction& other) = default;

    PhysicalFunction& operator=(const PhysicalFunction& other)
    {
        if (this != &other)
        {
            self = other.self;
        }
        return *this;
    }

private:
    struct Concept : PhysicalFunctionConcept
    {
        [[nodiscard]] virtual std::shared_ptr<Concept> clone() const = 0;
    };

    template <IsPhysicalFunction FunctionType>
    struct Model : Concept
    {
        FunctionType data;

        explicit Model(FunctionType d) : data(std::move(d)) { }

        [[nodiscard]] std::shared_ptr<Concept> clone() const override { return std::make_shared<Model>(data); }

        [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const override { return data.execute(record, arena); }
    };

    std::shared_ptr<Concept> self;
};
}
