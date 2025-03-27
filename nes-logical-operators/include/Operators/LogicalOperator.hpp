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
#include <variant>
#include <string_view>
#include <vector>
#include <API/Schema.hpp>
#include <Traits/Trait.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/Formatter.hpp>
#include <SerializableOperator.pb.h>
#include <ErrorHandling.hpp>

namespace NES
{
class SerializableOperator;

/// only used in this header, thus the anonymous namespace
namespace
{
inline OperatorId getNextOperatorId() {
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}
}

struct LogicalOperatorConcept
{
    virtual ~LogicalOperatorConcept() = default;

    explicit LogicalOperatorConcept() : id(getNextOperatorId()) {}
    explicit LogicalOperatorConcept(OperatorId existingId) : id(existingId) {}

    [[nodiscard]] virtual std::string toString() const = 0;
    [[nodiscard]] virtual std::vector<struct LogicalOperator> getChildren() const = 0;
    virtual void setChildren(std::vector<struct LogicalOperator>) = 0;

    [[nodiscard]] virtual bool operator==(struct LogicalOperatorConcept const& rhs) const = 0;

    /// Used to during planning and optimization for rule resolution
    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;
    [[nodiscard]] virtual SerializableOperator serialize() const = 0;
    [[nodiscard]] virtual Optimizer::TraitSet getTraitSet() const = 0;

    [[nodiscard]] virtual std::vector<Schema> getInputSchemas() const = 0;
    [[nodiscard]] virtual Schema getOutputSchema() const = 0;

    [[nodiscard]] virtual std::vector<std::vector<OriginId>> getInputOriginIds() const = 0;
    [[nodiscard]] virtual std::vector<OriginId> getOutputOriginIds() const = 0;
    virtual void setInputOriginIds(std::vector<std::vector<OriginId>>) = 0;
    virtual void setOutputOriginIds(std::vector<OriginId>) = 0;

    virtual bool inferSchema(Schema inputSchema) = 0;

    OperatorId id = INVALID_OPERATOR_ID;
};

/// Enables default construction of LogicalOperator.
/// Necessary to enable more ergonomic usage in e.g. unordered maps etc.
class NullLogicalOperator : public NES::LogicalOperatorConcept {
public:
    std::string toString() const override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }

    std::vector<NES::LogicalOperator> getChildren() const override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }

    void setChildren(std::vector<NES::LogicalOperator>) override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }
    bool operator==(NES::LogicalOperatorConcept const&) const override {
        return false;
    }
    std::string_view getName() const noexcept override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }
    NES::SerializableOperator serialize() const override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }
    Optimizer::TraitSet getTraitSet() const override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }
    std::vector<Schema> getInputSchemas() const override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }
    Schema getOutputSchema() const override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }
    std::vector<std::vector<OriginId>> getInputOriginIds() const override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }
    std::vector<OriginId> getOutputOriginIds() const override {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }

    void setInputOriginIds(std::vector<std::vector<OriginId>>) override
    {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }
    void setOutputOriginIds(std::vector<OriginId>) override
    {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }

    virtual bool inferSchema(Schema) override
    {
        PRECONDITION(false, "Calls in NullLogicalOperator are undefined");
    }

};

/// Id is preserved during copy
struct LogicalOperator {
public:
    template<typename T>
    LogicalOperator(const T& op) : self(std::make_unique<Model<T>>(op, op.id)) {}

    LogicalOperator() : self(std::make_unique<Model<NullLogicalOperator>>(NullLogicalOperator{})) {}

    LogicalOperator(const LogicalOperator& other) : self(other.self->clone()) {}

    template<typename T>
    const T* tryGet() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get())) {
            return &(p->data);
        }
        return nullptr;
    }

    template<typename T>
    const T* get() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get())) {
            return &(p->data);
        }
        return nullptr;
    }

    LogicalOperator(LogicalOperator&&) noexcept = default;

    LogicalOperator& operator=(const LogicalOperator& other) {
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

    [[nodiscard]] std::vector<LogicalOperator> getChildren() const {
        return self->getChildren();
    }

    void setChildren(std::vector<LogicalOperator> children) {
        self->setChildren(children);
    }

    [[nodiscard]] OperatorId getId() const {
        return self->id;
    }

    bool operator==(const LogicalOperator &other) const {
        return self->equals(*other.self);
    }

    std::string_view getName() const noexcept
    {
        return self->getName();
    }

    SerializableOperator serialize() const
    {
        return self->serialize();
    }

    Optimizer::TraitSet getTraitSet() const
    {
        return self->getTraitSet();
    }

    std::vector<Schema> getInputSchemas() const
    {
        return self->getInputSchemas();
    }

    Schema getOutputSchema() const
    {
        return self->getOutputSchema();
    }

    std::vector<std::vector<OriginId>> getInputOriginIds() const
    {
            return self->getInputOriginIds();
    }

    std::vector<OriginId> getOutputOriginIds() const
    {
        return self->getOutputOriginIds();
    }

    void setInputOriginIds(std::vector<std::vector<OriginId>> ids)
    {
        return self->setInputOriginIds(ids);
    }

    void setOutputOriginIds(std::vector<OriginId> ids)
    {
        return self->setOutputOriginIds(ids);
    }

    bool inferSchema(Schema inputSchema)
    {
        return self->inferSchema(inputSchema);
    }

private:
    struct Concept : LogicalOperatorConcept {
        explicit Concept(OperatorId existingId) : LogicalOperatorConcept(existingId) {}
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template<typename T>
    struct Model : Concept {
        T data;

        explicit Model(T d)
            : Concept(getNextOperatorId()), data(std::move(d)) {}

        Model(T d, OperatorId existingId)
            : Concept(existingId), data(std::move(d)) {}

        [[nodiscard]] std::unique_ptr<Concept> clone() const override {
            return std::make_unique<Model<T>>(data, this->id);
        }

        [[nodiscard]] std::string toString() const override
        {
            return data.toString();
        }

        [[nodiscard]] std::vector<LogicalOperator> getChildren() const override
        {
            return data.getChildren();
        }

        void setChildren(std::vector<LogicalOperator> children) override
        {
            data.setChildren(children);
        }

        [[nodiscard]] std::string_view getName() const noexcept override
        {
            return data.getName();
        }

        [[nodiscard]] SerializableOperator serialize() const override
        {
            return data.serialize();
        }

        [[nodiscard]] Optimizer::TraitSet getTraitSet() const override
        {
            return data.getTraitSet();
        }

        [[nodiscard]] std::vector<Schema> getInputSchemas() const override
        {
            return data.getInputSchemas();
        }

        [[nodiscard]] Schema getOutputSchema() const override
        {
            return data.getOutputSchema();
        }

        [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override
        {
            return data.getInputOriginIds();
        }

        [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override
        {
            return data.getOutputOriginIds();
        }

        void setInputOriginIds(std::vector<std::vector<OriginId>> ids) override
        {
            return data.setInputOriginIds(ids);
        }

        void setOutputOriginIds(std::vector<OriginId> ids) override
        {
            return data.setOutputOriginIds(ids);
        }

        bool inferSchema(Schema inputSchema) override
        {
            return data.inferSchema(inputSchema);
        }

        [[nodiscard]] bool operator==(LogicalOperatorConcept const& rhs) const override {
            if (auto* p = dynamic_cast<const Concept*>(&rhs)) {
                return equals(*p);
            }
            return false;
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

inline std::ostream& operator<<(std::ostream& os, const LogicalOperator& op)
{
    return os << op.toString();
}
}

/// Hash is based solely on unique identifier
namespace std {
template<>
struct hash<NES::LogicalOperator> {
    std::size_t operator()(const NES::LogicalOperator& op) const noexcept {
        return std::hash<NES::OperatorId>{}(op.getId());
    }
};
}
FMT_OSTREAM(NES::LogicalOperator);
