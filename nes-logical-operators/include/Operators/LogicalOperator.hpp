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
inline OperatorId getNextLogicalOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}
}

struct LogicalOperatorConcept
{
    virtual ~LogicalOperatorConcept() = default;

    explicit LogicalOperatorConcept();
    explicit LogicalOperatorConcept(OperatorId existingId);

    [[nodiscard]] virtual std::string toString() const = 0;
    [[nodiscard]] virtual std::vector<struct LogicalOperator> getChildren() const = 0;
    [[nodiscard]] virtual LogicalOperator withChildren(std::vector<struct LogicalOperator>) const = 0;

    [[nodiscard]] virtual bool operator==(struct LogicalOperatorConcept const& rhs) const = 0;

    /// Used to during planning and optimization for rule resolution
    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;
    [[nodiscard]] virtual SerializableOperator serialize() const = 0;
    [[nodiscard]] virtual Optimizer::TraitSet getTraitSet() const = 0;

    [[nodiscard]] virtual std::vector<Schema> getInputSchemas() const = 0;
    [[nodiscard]] virtual Schema getOutputSchema() const = 0;

    [[nodiscard]] virtual std::vector<std::vector<OriginId>> getInputOriginIds() const = 0;
    [[nodiscard]] virtual std::vector<OriginId> getOutputOriginIds() const = 0;
    [[nodiscard]] virtual LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>>) const = 0;
    [[nodiscard]] virtual LogicalOperator withOutputOriginIds(std::vector<OriginId>) const = 0;
    [[nodiscard]] virtual LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const = 0;

    OperatorId id = INVALID_OPERATOR_ID;
};

/// Enables default construction of LogicalOperator.
/// Necessary to enable more ergonomic usage in e.g. unordered maps etc.
class NullLogicalOperator : public LogicalOperatorConcept {
public:
    [[nodiscard]] std::string toString() const override;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const override;
    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator>) const override;
    [[nodiscard]] bool operator==(LogicalOperatorConcept const&) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] SerializableOperator serialize() const override;
    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override;
    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;
    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>>) const override;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId>) const override;
    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema>) const override;
};

/// Id is preserved during copy
struct LogicalOperator {
    template<typename T>
    LogicalOperator(const T& op) : self(std::make_unique<Model<T>>(op, op.id)) {}

    LogicalOperator();
    LogicalOperator(const LogicalOperator& other);

    template<typename T>
    [[nodiscard]] std::optional<T> tryGet() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get())) {
            return p->data;
        }
        return std::nullopt;
    }

    template<typename T>
    [[nodiscard]] const T get() const {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    LogicalOperator(LogicalOperator&&) noexcept;

    LogicalOperator& operator=(const LogicalOperator& other);

    [[nodiscard]] std::string toString() const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const;

    [[nodiscard]] OperatorId getId() const;

    [[nodiscard]] bool operator==(const LogicalOperator &other) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SerializableOperator serialize() const;
    [[nodiscard]] Optimizer::TraitSet getTraitSet() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const;

    [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const;
    [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const;
    [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    struct Concept : LogicalOperatorConcept {
        explicit Concept(OperatorId existingId) : LogicalOperatorConcept(existingId) {}
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template<typename T>
    struct Model : Concept {
        T data;

        explicit Model(T d) : Concept(getNextLogicalOperatorId()), data(std::move(d)) { }

        Model(T d, OperatorId existingId) : Concept(existingId), data(std::move(d)) { }

        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::make_unique<Model>(data, this->id); }

        [[nodiscard]] std::string toString() const override { return data.toString(); }

        [[nodiscard]] std::vector<LogicalOperator> getChildren() const override { return data.getChildren(); }

        [[nodiscard]] LogicalOperator withChildren(std::vector<LogicalOperator> children) const override
        {
            return data.withChildren(children);
        }

        [[nodiscard]] std::string_view getName() const noexcept override { return data.getName(); }

        [[nodiscard]] SerializableOperator serialize() const override { return data.serialize(); }

        [[nodiscard]] Optimizer::TraitSet getTraitSet() const override { return data.getTraitSet(); }

        [[nodiscard]] std::vector<Schema> getInputSchemas() const override { return data.getInputSchemas(); }

        [[nodiscard]] Schema getOutputSchema() const override { return data.getOutputSchema(); }

        [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override { return data.getInputOriginIds(); }

        [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override { return data.getOutputOriginIds(); }

        [[nodiscard]] LogicalOperator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override
        {
            return data.withInputOriginIds(ids);
        }

        [[nodiscard]] LogicalOperator withOutputOriginIds(std::vector<OriginId> ids) const override
        {
            return data.withOutputOriginIds(ids);
        }

        [[nodiscard]] LogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const override
        {
            return data.withInferredSchema(inputSchemas);
        }

        [[nodiscard]] bool operator==(const LogicalOperatorConcept& rhs) const override
        {
            if (const auto* p = dynamic_cast<const Concept*>(&rhs))
            {
                return equals(*p);
            }
            return false;
        }

        [[nodiscard]] bool equals(const Concept& other) const override {
            if (auto p = dynamic_cast<const Model*>(&other)) {
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
