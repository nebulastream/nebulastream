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
#include <string>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Traits/Trait.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>
#include <SerializableOperator.pb.h>
#include <Util/PlanRenderer.hpp>

using namespace NES;

namespace
{
inline OperatorId getNextLogicalOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}
}

namespace NES::Logical
{

/// Concept defining the interface for all logical operators in the query plan.
/// This concept defines the common interface that all logical operators must implement.
/// Logical operators represent operations in the query plan and are used during query
/// planning and optimization.
struct OperatorConcept
{
    virtual ~OperatorConcept() = default;

    explicit OperatorConcept();
    explicit OperatorConcept(OperatorId existingId);

    /// Returns a string representation of the operator
    [[nodiscard]] virtual std::string explain(ExplainVerbosity verbosity) const = 0;
    
    /// Returns the children operators of this operator
    [[nodiscard]] virtual std::vector<struct Operator> getChildren() const = 0;
    
    /// Creates a new operator with the given children
    [[nodiscard]] virtual Operator withChildren(std::vector<struct Operator>) const = 0;

    /// Compares this operator with another for equality
    [[nodiscard]] virtual bool operator==(struct OperatorConcept const& rhs) const = 0;

    /// Returns the name of the operator, used during planning and optimization
    [[nodiscard]] virtual std::string_view getName() const noexcept = 0;
    
    /// Serializes the operator to a protobuf message
    [[nodiscard]] virtual SerializableOperator serialize() const = 0;
    
    /// Returns the trait set of the operator
    [[nodiscard]] virtual Optimizer::TraitSet getTraitSet() const = 0;

    /// Returns the input schemas of the operator
    [[nodiscard]] virtual std::vector<Schema> getInputSchemas() const = 0;
    
    /// Returns the output schema of the operator
    [[nodiscard]] virtual Schema getOutputSchema() const = 0;

    /// Returns the input origin IDs of the operator
    [[nodiscard]] virtual std::vector<std::vector<OriginId>> getInputOriginIds() const = 0;
    
    /// Returns the output origin IDs of the operator
    [[nodiscard]] virtual std::vector<OriginId> getOutputOriginIds() const = 0;
    
    /// Creates a new operator with the given input origin IDs
    [[nodiscard]] virtual Operator withInputOriginIds(std::vector<std::vector<OriginId>>) const = 0;
    
    /// Creates a new operator with the given output origin IDs
    [[nodiscard]] virtual Operator withOutputOriginIds(std::vector<OriginId>) const = 0;
    
    /// Creates a new operator with inferred schema based on input schemas
    [[nodiscard]] virtual Operator withInferredSchema(std::vector<Schema> inputSchemas) const = 0;

    /// Unique identifier for this operator
    /// Currently not const to allow serialization.
    OperatorId id = INVALID_OPERATOR_ID;
};

/// A type-erased wrapper for operators.
/// This class provides type erasure for operators, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual operator implementation.
/// @tparam T The type of the operator. Must inherit from OperatorConcept.
template<typename T>
concept IsOperator = std::is_base_of_v<OperatorConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Enables default construction of Operator.
/// Necessary to enable more ergonomic usage in e.g. default constructable of unordered maps etc.
class NullOperator : public OperatorConcept
{
public:
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override;
    [[nodiscard]] std::vector<Operator> getChildren() const override;
    [[nodiscard]] Operator withChildren(std::vector<Operator>) const override;
    [[nodiscard]] bool operator==(const OperatorConcept&) const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] NES::SerializableOperator serialize() const override;
    [[nodiscard]] Optimizer::TraitSet getTraitSet() const override;
    [[nodiscard]] std::vector<Schema> getInputSchemas() const override;
    [[nodiscard]] Schema getOutputSchema() const override;
    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const override;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const override;
    [[nodiscard]] Operator withInputOriginIds(std::vector<std::vector<OriginId>>) const override;
    [[nodiscard]] Operator withOutputOriginIds(std::vector<OriginId>) const override;
    [[nodiscard]] Operator withInferredSchema(std::vector<Schema>) const override;
};

/// Id is preserved during copy
struct Operator
{
    /// Constructs an Operator from a concrete operator type.
    /// @tparam T The type of the operator. Must satisfy IsOperator concept.
    /// @param op The operator to wrap.
    template <IsOperator T>
    Operator(const T& op) : self(std::make_unique<Model<T>>(op, op.id))
    {
    }

    Operator();
    Operator(const Operator& other);

    /// Attempts to get the underlying operator as type T.
    /// @tparam T The type to try to get the operator as.
    /// @return std::optional<T> The operator if it is of type T, nullopt otherwise.
    template <typename T>
    [[nodiscard]] std::optional<T> tryGet() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        return std::nullopt;
    }

    /// Gets the underlying operator as type T.
    /// @tparam T The type to get the operator as.
    /// @return const T The operator.
    /// @throw InvalidDynamicCast If the operator is not of type T.
    template <typename T>
    [[nodiscard]] const T get() const
    {
        if (auto p = dynamic_cast<const Model<T>*>(self.get()))
        {
            return p->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(T).name(), typeid(self).name());
    }

    Operator(Operator&&) noexcept;

    Operator& operator=(const Operator& other);

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] std::vector<Operator> getChildren() const;
    [[nodiscard]] Operator withChildren(std::vector<Operator> children) const;

    [[nodiscard]] OperatorId getId() const;

    [[nodiscard]] bool operator==(const Operator& other) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] SerializableOperator serialize() const;
    [[nodiscard]] Optimizer::TraitSet getTraitSet() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::vector<std::vector<OriginId>> getInputOriginIds() const;
    [[nodiscard]] std::vector<OriginId> getOutputOriginIds() const;

    [[nodiscard]] Operator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const;
    [[nodiscard]] Operator withOutputOriginIds(std::vector<OriginId> ids) const;
    [[nodiscard]] Operator withInferredSchema(std::vector<Schema> inputSchemas) const;

private:
    struct Concept : OperatorConcept
    {
        explicit Concept(OperatorId existingId) : OperatorConcept(existingId) { }
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual bool equals(const Concept& other) const = 0;
    };

    template <typename T>
    struct Model : Concept
    {
        T data;

        explicit Model(T d) : Concept(getNextLogicalOperatorId()), data(std::move(d)) { }

        Model(T d, OperatorId existingId) : Concept(existingId), data(std::move(d)) { }

        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::make_unique<Model>(data, this->id); }

        [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const override { return data.explain(verbosity); }

        [[nodiscard]] std::vector<Operator> getChildren() const override { return data.getChildren(); }

        [[nodiscard]] Operator withChildren(std::vector<Operator> children) const override
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

        [[nodiscard]] Operator withInputOriginIds(std::vector<std::vector<OriginId>> ids) const override
        {
            return data.withInputOriginIds(ids);
        }

        [[nodiscard]] Operator withOutputOriginIds(std::vector<OriginId> ids) const override
        {
            return data.withOutputOriginIds(ids);
        }

        [[nodiscard]] Operator withInferredSchema(std::vector<Schema> inputSchemas) const override
        {
            return data.withInferredSchema(inputSchemas);
        }

        [[nodiscard]] bool operator==(const OperatorConcept& rhs) const override
        {
            if (const auto* p = dynamic_cast<const Concept*>(&rhs))
            {
                return equals(*p);
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
    };

    std::unique_ptr<Concept> self;
};

inline std::ostream& operator<<(std::ostream& os, const Operator& op)
{
    return os << op.explain(ExplainVerbosity::Short);
}
}

/// Hash is based solely on unique identifier
namespace std
{
template <>
struct hash<NES::Logical::Operator>
{
    std::size_t operator()(const NES::Logical::Operator& op) const noexcept { return std::hash<NES::OperatorId>{}(op.getId()); }
};
}
FMT_OSTREAM(NES::Logical::Operator);
