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
#include <ExecutionContext.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Functions
{
using namespace Nautilus;

struct PhysicalFunctionConcept
{
    virtual ~PhysicalFunctionConcept() = default;
    [[nodiscard]] virtual VarVal execute(const Record& record, ArenaRef& arena) const = 0;
};

struct PhysicalFunction {
public:
    template<typename T>
    PhysicalFunction(const T& op) : self(std::make_unique<Model<T>>(op)) {}

    PhysicalFunction(const PhysicalFunction& other)
        : self(other.self->clone()) {}

    template<typename T>
    const T& get() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        INVARIANT(false, "Bad cast: requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    template<typename T>
    const T* tryGet() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get())) {
            return &(p->data);
        }
        return nullptr;
    }

    PhysicalFunction(PhysicalFunction&&) noexcept = default;

    PhysicalFunction& operator=(const PhysicalFunction& other) {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    bool operator==(const PhysicalFunction &other) const {
        return self->equals(*other.self);
    }

    VarVal execute(const Record& record, ArenaRef& arena) const
    {
        return self->execute(record, arena);
    }

private:
    struct Concept : PhysicalFunctionConcept {
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

        VarVal execute(const Record& record, ArenaRef& arena) const override
        {
            return data.execute(record, arena);
        }

        [[nodiscard]] bool equals(const Concept& other) const override {
            if (auto p = dynamic_cast<const Model<T>*>(&other)) {
                return data.operator==(p->data);
            }
            return false;
        }
    };

    std::unique_ptr<Concept> self;
};
}
