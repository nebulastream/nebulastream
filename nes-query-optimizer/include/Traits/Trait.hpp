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
#include <memory>
#include <optional>
#include <set>
#include <ErrorHandling.hpp>

namespace NES
{
struct LogicalOperator;
}

namespace NES::Optimizer
{

struct TraitConcept
{
    virtual ~TraitConcept() = default;
    [[nodiscard]] virtual const std::type_info& getType() const = 0;
    virtual bool operator==(const TraitConcept& other) const = 0;
};

struct Trait
{
public:
    template <typename T>
    Trait(const T& op) : self(std::make_unique<Model<T>>(op))
    {
    }

    Trait(const Trait& other) : self(other.self->clone()) { }

    Trait(Trait&&) noexcept = default;

    template <typename T>
    [[nodiscard]] std::optional<T> tryGet() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        return std::nullopt;
    }

    template <typename T>
    [[nodiscard]] const T get() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    Trait& operator=(const Trait& other)
    {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    [[nodiscard]] const std::type_info& getType() const { return self->getType(); }

private:
    struct Concept : TraitConcept
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

        bool operator==(const TraitConcept& other) const override
        {
            if (auto p = dynamic_cast<const Model*>(&other))
            {
                return data == p->data;
            }
            return false;
        }

        [[nodiscard]] bool equals(const Concept& other) const override
        {
            if (auto p = dynamic_cast<const Model*>(&other))
            {
                return data == p->data;
            }
            return false;
        }

        [[nodiscard]] const std::type_info& getType() const override { return data.getType(); }
    };

    std::unique_ptr<Concept> self;
};

using TraitSet = std::vector<Trait>;
}

template <typename T>
bool hasTrait(const NES::Optimizer::TraitSet& traitSet)
{
    for (const auto& trait : traitSet)
    {
        if (trait.tryGet<T>())
        {
            return true;
        }
    }
    return false;
}

template <typename... TraitTypes>
bool hasTraits(const NES::Optimizer::TraitSet& traitSet)
{
    return (hasTrait<TraitTypes>(traitSet) && ...);
}
