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

namespace NES
{
struct LogicalOperator;
}

namespace NES::Optimizer
{

struct TraitConcept
{
    virtual ~TraitConcept() = default;
    virtual const std::type_info& getType() const = 0;
    virtual bool operator==(const TraitConcept& other) const = 0;
};

struct Trait {
public:
    template<typename T>
    Trait(const T& op) : self(std::make_unique<Model<T>>(op)) {}

    Trait(const Trait& other)
        : self(other.self->clone()) {}

    Trait(Trait&&) noexcept = default;

    Trait& operator=(const Trait& other) {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    [[nodiscard]] const std::type_info& getType() const {
        return self->getType();
    }

private:
    struct Concept : TraitConcept {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template<typename T>
    struct Model : Concept {
        T data;
        explicit Model(T d) : data(std::move(d)) {}

        [[nodiscard]] std::unique_ptr<Concept> clone() const override {
            return std::unique_ptr<Concept>(new Model<T>(data));
        }

        [[nodiscard]] bool equals(const Concept& other) const override {
            if (auto p = dynamic_cast<const Model<T>*>(&other)) {
                return data == p->data;
            }
            return false;
        }

        [[nodiscard]] const std::type_info& getType() const override {
            return data.getType();
        }
    };

    std::unique_ptr<Concept> self;
};

using TraitSet = std::set<std::unique_ptr<Trait>>;
}

template <typename T>
bool hasTrait(const NES::Optimizer::TraitSet& traitSet) {
    for (const auto& traitPtr : traitSet) {
        if (traitPtr && traitPtr->getType() == typeid(T)) {
            return true;
        }
    }
    return false;
}

template <typename... TraitTypes>
bool hasTraits(const NES::Optimizer::TraitSet& traitSet) {
    return (hasTrait<TraitTypes>(traitSet) && ...);
}
