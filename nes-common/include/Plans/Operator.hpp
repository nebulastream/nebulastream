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
#include <atomic>
#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>

namespace NES {

/// only used in this header, thus the anonymous namespace
namespace
{
inline OperatorId getNextOperatorId() {
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}
}

struct OperatorConcept {
    virtual ~OperatorConcept() = default;
    [[nodiscard]] virtual std::string toString() const = 0;
    [[nodiscard]] virtual std::vector<struct Operator> getChildren() const = 0;
    virtual void setChildren(std::vector<struct Operator>) = 0;

    const OperatorId id = getNextOperatorId();
};


struct Operator {
public:
    template<typename T>
    Operator(const T& op) : self(std::make_unique<Model<T>>(op)) {}

    Operator(const Operator& other)
        : self(other.self->clone()) {}

    template<typename T>
    const T* get() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get())) {
            return &(p->data);
        }
        return nullptr;
    }

    Operator(Operator&&) noexcept = default;

    Operator& operator=(const Operator& other) {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    [[nodiscard]] std::string toString() const
    {
        return self->toString();
    }

    [[nodiscard]] std::vector<Operator> getChildren() const {
        return self->getChildren();
    }

    void setChildren(std::vector<Operator> children) {
        self->setChildren(children);
    }

    [[nodiscard]] OperatorId getId() const {
        return self->id;
    }

    bool operator==(const Operator &other) const {
        return self->equals(*other.self);
    }

private:
    struct Concept : OperatorConcept {
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

        [[nodiscard]] std::string toString() const override
        {
            return data.toString();
        }

        [[nodiscard]] std::vector<Operator> getChildren() const override
        {
            return data.getChildren();
        }

        void setChildren(std::vector<Operator> children) override
        {
            data.setChildren(children);
        }

        [[nodiscard]] bool equals(const Concept& other) const override {
            if (auto p = dynamic_cast<const Model<T>*>(&other)) {
                return data == p->data;
            }
            return false;
        }
    };

    std::unique_ptr<Concept> self;
};

inline std::ostream& operator<<(std::ostream& os, const Operator& op)
{
    return os << op.toString();
}
}

/// Hash is based solely on unique identifier
namespace std {
template<>
struct hash<NES::Operator> {
    std::size_t operator()(const NES::Operator& op) const noexcept {
        return std::hash<NES::OperatorId>{}(op.getId());
    }
};
}
FMT_OSTREAM(NES::Operator);
