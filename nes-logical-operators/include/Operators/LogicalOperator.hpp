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

#include <atomic>
#include <string_view>
#include <variant>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Formatter.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{
class SerializableOperator;

/// only used in this header, thus the anonymous namespace
namespace
{
inline OperatorId getNextOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}
}

struct LogicalOperatorConcept
{
    virtual ~LogicalOperatorConcept() = default;

    [[nodiscard]] virtual std::string toString() const = 0;
    [[nodiscard]] virtual std::vector<struct LogicalOperator> getChildren() const = 0;
    virtual void setChildren(std::vector<struct LogicalOperator>) = 0;

    [[nodiscard]] virtual bool operator==(struct LogicalOperatorConcept const& rhs) const = 0;

    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;
    [[nodiscard]] virtual SerializableOperator serialize() const = 0;
    [[nodiscard]] virtual Optimizer::TraitSet getTraitSet() const = 0;

    [[nodiscard]] virtual std::vector<Schema> getInputSchemas() const = 0;
    [[nodiscard]] virtual Schema getOutputSchema() const = 0;

    [[nodiscard]] virtual std::vector<std::vector<OriginId>> getInputOriginIds() const = 0;
    [[nodiscard]] virtual std::vector<OriginId> getOutputOriginIds() const = 0;

    const OperatorId id = getNextOperatorId();
};

struct LogicalOperator
{
public:
    template <typename T>
    LogicalOperator(const T& op) : self(std::make_unique<Model<T>>(op))
    {
    }

    LogicalOperator(const LogicalOperator& other) : self(other.self->clone()) { }

    template <typename T>
    const T* tryGet() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return &(p->data);
        }
        return nullptr;
    }

    template <typename T>
    const T* get() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return &(p->data);
        }
        return nullptr;
    }

    LogicalOperator(LogicalOperator&&) noexcept = default;

    LogicalOperator& operator=(const LogicalOperator& other)
    {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    [[nodiscard]] std::string toString() const { return self->toString(); }

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const { return self->getChildren(); }

    void setChildren(std::vector<LogicalOperator> children) { self->setChildren(children); }

    [[nodiscard]] OperatorId getId() const { return self->id; }

    bool operator==(const LogicalOperator& other) const { return self->equals(*other.self); }

    std::string_view getName() const noexcept { return self->getName(); }

    SerializableOperator serialize() const { return self->serialize(); }

    Optimizer::TraitSet getTraitSet() const { return self->getTraitSet(); }

    std::vector<Schema> getInputSchemas() const { return self->getInputSchemas(); }

    Schema getOutputSchema() const { return self->getOutputSchema(); }

    std::vector<std::vector<OriginId>> getInputOriginIds() const { return self->getInputOriginIds(); }

    std::vector<OriginId> getOutputOriginIds() const { return self->getOutputOriginIds(); }

private:
    struct Concept : LogicalOperatorConcept
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

        [[nodiscard]] std::vector<LogicalOperator> getChildren() const override { return data.getChildren(); }

        void setChildren(std::vector<LogicalOperator> children) override { data.setChildren(children); }

        [[nodiscard]] std::string_view getName() const noexcept override { return data.getName(); }

        [[nodiscard]] SerializableOperator serialize() const override { return data.serialize(); }

        [[nodiscard]] Optimizer::TraitSet getTraitSet() const override { return data.getTraitSet(); }

        [[nodiscard]] std::vector<Schema> getInputSchemas() const override { return data.getInputSchemas(); }

        [[nodiscard]] Schema getOutputSchema() const override { return data.getOutputSchema(); }

        [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override { return data.getInputOriginIds(); }

        [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override { return data.getOutputOriginIds(); }

        [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override
        {
            if (auto* p = dynamic_cast<const Concept*>(&rhs))
            {
                return equals(*p);
            }
            return false;
        }

        [[nodiscard]] bool equals(const Concept& other) const override
        {
            if (auto p = dynamic_cast<const Model<T>*>(&other))
            {
                return data.operator==(p->data);
            }
            return false;
        }
    };

    std::unique_ptr<Concept> self;
};

inline std::ostream& operator<<(std::ostream& os, const LogicalOperator& op)
{
    return os << op.toString();
}
}

/// Hash is based solely on unique identifier
namespace std
{
template <>
struct hash<NES::LogicalOperator>
{
    std::size_t operator()(const NES::LogicalOperator& op) const noexcept { return std::hash<NES::OperatorId>{}(op.getId()); }
};
}
FMT_OSTREAM(NES::LogicalOperator);
