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

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <unordered_set>
#include <nameof.hpp>

#include <Util/PlanRenderer.hpp>

#include <ErrorHandling.hpp>
#include <SerializableTrait.pb.h>

namespace NES
{

/// Concept defining the interface for all traits in the query optimizer.
/// Traits are used to annotate logical operators with additional properties
/// that can be used during query optimization.
struct TraitConcept
{
    virtual ~TraitConcept() = default;

    /// Returns the type information of this trait.
    /// @return const std::type_info& The type information of this trait.
    [[nodiscard]] virtual const std::type_info& getType() const = 0;
    [[nodiscard]] virtual std::string_view getName() const = 0;

    /// Serializes this trait to a protobuf message.
    /// @return SerializableTrait The serialized trait.
    [[nodiscard]] virtual SerializableTrait serialize() const = 0;

    /// Compares this trait with another trait for equality.
    /// @param other The trait to compare with.
    /// @return bool True if the traits are equal, false otherwise.
    virtual bool operator==(const TraitConcept& other) const = 0;

    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;

    /// Computes the hash value for this trait.
    /// @return size_t The hash value.
    [[nodiscard]] virtual size_t hash() const = 0;
};

/// Base class providing default implementations for traits without custom data
template <typename Derived>
struct DefaultTrait : TraitConcept
{
    bool operator==(const TraitConcept& other) const final { return typeid(other) == typeid(Derived); }

    [[nodiscard]] size_t hash() const final { return std::type_index(typeid(Derived)).hash_code(); }

    [[nodiscard]] const std::type_info& getType() const final { return typeid(Derived); }

    [[nodiscard]] SerializableTrait serialize() const final { return SerializableTrait{}; }

    [[nodiscard]] std::string explain(ExplainVerbosity) const final { return "DefaultTrait"; }

    friend Derived;

private:
    DefaultTrait() = default;
};

/// A type-erased wrapper for traits.
/// C.f.: https://sean-parent.stlab.cc/presentations/2017-01-18-runtime-polymorphism/2017-01-18-runtime-polymorphism.pdf
/// This class provides type erasure for traits, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual trait implementation.
/// @tparam T The type of the trait. Must inherit from TraitConcept.
template <typename T>
concept IsTrait = std::is_base_of_v<TraitConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Type-erased trait that can be used to annotate logical operators.
struct Trait
{
    /// Constructs a Trait from a concrete trait type.
    /// @tparam TraitType The type of the trait. Must satisfy IsTrait concept.
    /// @param op The trait to wrap.
    template <IsTrait TraitType>
    Trait(const TraitType& op) : self(std::make_unique<Model<TraitType>>(op)) /// NOLINT
    {
    }

    Trait(const Trait& other);
    Trait(Trait&&) noexcept;

    bool operator==(const Trait& other) const { return self->equals(*other.self); }

    [[nodiscard]] size_t hash() const { return self->hash(); }

    /// Attempts to get the underlying trait as type TraitType.
    /// @tparam TraitType The type to try to get the trait as.
    /// @return std::optional<TraitType> The trait if it is of type TraitType, nullopt otherwise.
    template <IsTrait TraitType>
    [[nodiscard]] std::optional<TraitType> tryGet() const
    {
        if (auto p = dynamic_cast<const Model<TraitType>*>(self.get()))
        {
            return p->data;
        }
        return std::nullopt;
    }

    /// Gets the underlying trait as type T.
    /// @tparam T The type to get the trait as.
    /// @return const T The trait.
    /// @throw InvalidDynamicCast If the trait is not of type T.
    template <typename T>
    [[nodiscard]] T get() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", NAMEOF_TYPE(T), NAMEOF_TYPE_EXPR(self));
    }

    Trait& operator=(const Trait& other);

    /// Serializes this trait to a protobuf message.
    /// @return SerializableTrait The serialized trait.
    [[nodiscard]] SerializableTrait serialize() const;

    [[nodiscard]] const std::type_info& getTypeInfo() const;
    [[nodiscard]] std::string_view getName() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

private:
    struct Concept : TraitConcept
    {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template <IsTrait TraitType>
    struct Model : Concept
    {
        TraitType data;

        explicit Model(TraitType d) : data(std::move(d)) { }

        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::make_unique<Model>(data); }

        bool operator==(const TraitConcept& other) const override
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

        [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return data.explain(verbosity); }

        [[nodiscard]] size_t hash() const override { return data.hash(); }

        [[nodiscard]] const std::type_info& getType() const override { return data.getType(); }

        [[nodiscard]] std::string_view getName() const override { return data.getName(); }

        [[nodiscard]] SerializableTrait serialize() const override { return data.serialize(); }
    };

    std::unique_ptr<Concept> self;
};

}

template <>
struct std::hash<NES::Trait>
{
    size_t operator()(const NES::Trait& trait) const noexcept { return trait.hash(); }
};
