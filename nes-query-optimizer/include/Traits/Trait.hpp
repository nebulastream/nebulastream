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
#include <set>
#include <Identifiers/Identifiers.hpp>

namespace NES
{
struct Operator;
}

namespace NES::Optimizer
{

struct TraitConcept
{
    virtual ~TraitConcept() = default;
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

    Trait& operator=(const Trait& other)
    {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    [[nodiscard]] std::string toString() const { return self->toString(); }

    [[nodiscard]] std::vector<Operator> getChildren() const { return self->getChildren(); }

    void setChildren(std::vector<Operator> children) { return self->setChildren(children); }

    OperatorId getId() const { return self->id; }

private:
    struct Concept : OperatorConcept
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

        [[nodiscard]] std::string toString() const override { return data.toString(); }

        [[nodiscard]] std::vector<Operator> getChildren() const override { return data.getChildren(); }

        void setChildren(std::vector<Operator> children) { data.setChildren(children); }

        [[nodiscard]] bool equals(const Concept& other) const override
        {
            if (auto p = dynamic_cast<const Model*>(&other))
            {
                return data == p->data;
            }
            return false;
        }
    };

    std::unique_ptr<Concept> self;
};


using TraitSet = std::set<std::unique_ptr<Trait>>;

template <typename T>
bool hasTrait(const TraitSet& traitSet)
{
    return std::any_of(traitSet.begin(), traitSet.end(), [](const auto& trait) { return dynamic_cast<T*>(trait.get()) != nullptr; });
}

template <typename... TraitTypes>
bool hasTraits(const TraitSet& traitSet)
{
    return (hasTrait<TraitTypes>(traitSet) && ...);
}

}
