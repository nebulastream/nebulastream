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
#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <rfl/flexbuf.hpp>
#include <ErrorHandling.hpp>
#include <SerializableTrait.pb.h>
#include <rfl.hpp>

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
    [[nodiscard]] virtual SerializableTrait generalSerialize() const = 0;

    /// Deserializes this trait from a protobuf message.
    virtual void generalDeserialize(const SerializableTrait&) = 0;

    virtual bool equals(const TraitConcept& other) const = 0;

    /// Computes the hash value for this trait.
    /// @return size_t The hash value.
    [[nodiscard]] virtual size_t generalHash() const = 0;
};

template <class T>
std::size_t generate_hash(const T& obj)
{
    std::size_t hash = 0;

    // Create a view of the object and apply hash function to each field
    const auto view = rfl::to_view(obj);
    view.apply(
        [&hash](const auto& field)
        {
            // field.value() gives us a pointer to the actual field value
            hash = folly::hash::hash_combine(hash, *field.value());
        });

    return hash;
}


template <class T>
bool objects_equal(const T& lhs, const T& rhs)
{
    // Create views of both objects
    const auto lhs_view = rfl::to_view(lhs);
    const auto rhs_view = rfl::to_view(rhs);

    // Compare field by field
    bool are_equal = true;

    return rfl::apply(
        [&]<typename... AFields>(AFields... aFields)
        { return rfl::apply([&]<typename... BFields>(BFields... bFields) { return ((*aFields == *bFields) && ...); }, rhs_view.values()); },
        lhs_view.values());
}

/// Base class providing default implementations for traits without custom data
template <typename Derived>
struct DefaultTrait : TraitConcept
{
    static constexpr bool HasData = requires(Derived d) {
        { d.data };
        requires std::is_standard_layout_v<decltype(d.data)>;
    };

    bool equals(const TraitConcept& other) const final
    {
        if (auto p = dynamic_cast<const Derived*>(&other))
        {
            if constexpr (HasData)
            {
                return objects_equal(static_cast<const Derived*>(this)->data, p->data);
            }
            else
            {
                return true;
            }
        }
        return false;
    }

    [[nodiscard]] size_t generalHash() const final
    {
        auto hash = std::type_index(typeid(Derived)).hash_code();
        if constexpr (HasData)
        {
            hash = folly::hash::hash_combine(hash, generate_hash(static_cast<const Derived*>(this)->data));
        }
        return hash;
    }

    [[nodiscard]] const std::type_info& getType() const final { return typeid(Derived); }
    [[nodiscard]] std::string_view getName() const final { return Derived::Name; }

    [[nodiscard]] SerializableTrait generalSerialize() const final
    {
        SerializableTrait trait;
        if constexpr (HasData)
        {
            const auto bytes = rfl::flexbuf::write(static_cast<const Derived*>(this)->data);
            trait.set_data(bytes.data(), bytes.size());
        }

        trait.set_trait_type(getName());
        return trait;
    }

    void generalDeserialize(const SerializableTrait& serializable) final
    {
        INVARIANT(serializable.trait_type() == getName(), "trait type mismatch");
        if constexpr (HasData)
        {
            auto result = rfl::flexbuf::read<decltype(Derived::data)>(serializable.data().data(), serializable.data().size());
            if (!result)
            {
                throw CannotDeserialize("Trait deserialization failed for '{}'", getName());
            }
            static_cast<Derived*>(this)->data = *result;
        }
    }
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

    [[nodiscard]] size_t hash() const { return self->generalHash(); }

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
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    Trait& operator=(const Trait& other);

    /// Serializes this trait to a protobuf message.
    /// @return SerializableTrait The serialized trait.
    [[nodiscard]] SerializableTrait serialize() const;
    void deserialize(const SerializableTrait&);
    std::type_index getTypeIndex() const { return self->getType(); }

private:
    struct Concept : TraitConcept
    {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
    };

    template <IsTrait TraitType>
    struct Model : Concept
    {
        TraitType data;
        explicit Model(TraitType d) : data(std::move(d)) { }

        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::make_unique<Model>(data); }

        bool equals(const TraitConcept& other) const override
        {
            if (auto model = dynamic_cast<const Model*>(&other))
            {
                return data.equals(model->data);
            }
            return false;
        }

        [[nodiscard]] size_t generalHash() const override { return data.generalHash(); }

        [[nodiscard]] const std::type_info& getType() const override { return data.getType(); }
        [[nodiscard]] std::string_view getName() const override { return data.getName(); }
        [[nodiscard]] SerializableTrait generalSerialize() const override { return data.generalSerialize(); }
        void generalDeserialize(const SerializableTrait& trait) override { data.generalDeserialize(trait); }
    };

    std::unique_ptr<Concept> self;
};

using TraitSet = std::unordered_map<std::type_index, Trait>;

}

template <>
struct std::hash<NES::Trait>
{
    size_t operator()(const NES::Trait& trait) const noexcept { return trait.hash(); }
};

namespace NES
{
template <IsTrait T>
std::optional<T> getTrait(const NES::TraitSet& traitSet)
{
    auto it = traitSet.find(std::type_index(typeid(T)));
    if (it != traitSet.end())
    {
        return it->second.get<T>();
    }
    return std::nullopt;
}

template <IsTrait T, typename... Args>
requires(std::is_constructible_v<T, Args...>)
TraitSet addTrait(const NES::TraitSet& traits, Args&&... args)
{
    TraitSet copy = traits;
    copy.try_emplace(typeid(T), T(std::forward<Args>(args)...));
    return copy;
}

template <IsTrait T, typename... Args>
requires(std::is_constructible_v<T, Args...>)
TraitSet addTrait(NES::TraitSet&& traits, Args&&... args)
{
    traits.try_emplace(typeid(T), T(std::forward<Args>(args)...));
    return traits;
}

TraitSet addTrait(TraitSet&& traits, Trait t);

template <IsTrait T>
bool hasTrait(const NES::TraitSet& traitSet)
{
    return traitSet.contains(std::type_index(typeid(T)));
}

template <typename... TraitTypes>
bool hasTraits(const NES::TraitSet& traitSet)
{
    return (hasTrait<TraitTypes>(traitSet) && ...);
}

TraitSet deserializeTraitSet(const SerializableTraitSet& traitSet);
void serializeTraitSet(const TraitSet& traitSet, SerializableTraitSet& traitSetProto);

}