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
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/PlanRenderer.hpp>
#include <boost/core/demangle.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
using namespace Nautilus;
struct ExecutionContext;

/// Unique ID generation for physical operators.
inline OperatorId getNextPhysicalOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}

/// Concept defining the interface for all physical operators in the query plan.
/// Physical operators represent operations that are executed during query execution.
/// TODO #875: Investigate C++20 Concepts to replace Operator/Function Inheritance
struct PhysicalOperatorConcept
{
    virtual ~PhysicalOperatorConcept() = default;

    explicit PhysicalOperatorConcept();
    explicit PhysicalOperatorConcept(OperatorId existingId);

    [[nodiscard]] virtual std::optional<struct PhysicalOperator> getChild() const = 0;
    virtual void setChild(struct PhysicalOperator child) = 0;

    /// This is called once before the operator starts processing records.
    /// We pass the nautilus::engine  give the possibility to the operator to create custom executable nautilus functions
    virtual void setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const;

    /// Opens the operator for processing records.
    /// This is called before each batch of records is processed.
    virtual void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// Closes the operator after processing records.
    /// This is called after each batch of records is processed.
    virtual void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// Terminates the operator.
    /// This is called once after all records have been processed.
    virtual void terminate(ExecutionContext& executionCtx) const;

    /// Executes the operator on the given record.
    virtual void execute(ExecutionContext& executionCtx, Record& record) const;

    /// Unique identifier for this operator.
    const OperatorId id = INVALID_OPERATOR_ID;

protected:
    /// Helper classes to propagate to the child
    void setupChild(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const;
    void openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void executeChild(ExecutionContext& executionCtx, Record& record) const;
    void terminateChild(ExecutionContext& executionCtx) const;
};

/// A type-erased wrapper for physical operators.
/// C.f.: https://sean-parent.stlab.cc/presentations/2017-01-18-runtime-polymorphism/2017-01-18-runtime-polymorphism.pdf
/// This class provides type erasure for physical operators, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual operator implementation.
/// @tparam T The type of the physical operator. Must inherit from PhysicalOperatorConcept.
template <typename T>
concept IsPhysicalOperator = std::is_base_of_v<PhysicalOperatorConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Type-erased physical operator that can be used to process records.
struct PhysicalOperator
{
    /// Constructs a PhysicalOperator from a concrete operator type.
    /// @tparam T The type of the operator. Must satisfy IsPhysicalOperator concept.
    /// @param op The operator to wrap.
    template <IsPhysicalOperator T>
    PhysicalOperator(const T& op) : self(std::make_shared<Model<T>>(op, op.id)) /// NOLINT
    {
    }

    PhysicalOperator();
    PhysicalOperator(const PhysicalOperator& other);
    PhysicalOperator(PhysicalOperator&&) noexcept;

    PhysicalOperator& operator=(const PhysicalOperator& other);

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const;
    void setChild(PhysicalOperator child);

    void setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;
    void terminate(ExecutionContext& executionCtx) const;
    void execute(ExecutionContext& executionCtx, Record& record) const;
    [[nodiscard]] std::string toString() const;

    [[nodiscard]] OperatorId getId() const;

    /// Attempts to get the underlying operator as type OperatorType.
    /// @tparam OperatorType The type to try to get the operator as.
    /// @return std::optional<OperatorType> The operator if it is of type OperatorType, nullopt otherwise.
    template <IsPhysicalOperator OperatorType>
    [[nodiscard]] std::optional<OperatorType> tryGet() const
    {
        if (auto p = dynamic_cast<const Model<OperatorType>*>(self.get()))
        {
            return p->data;
        }
        return std::nullopt;
    }

    /// Gets the underlying operator as type OperatorType.
    /// @tparam OperatorType The type to get the operator as.
    /// @return OperatorType The operator.
    /// @throw InvalidDynamicCast If the operator is not of type OperatorType.
    template <IsPhysicalOperator OperatorType>
    [[nodiscard]] OperatorType get() const
    {
        if (auto p = dynamic_cast<const Model<OperatorType>*>(self.get()))
        {
            return p->data;
        }
        throw InvalidDynamicCast("requested type {} , but stored type is {}", typeid(OperatorType).name(), typeid(self).name());
    }

private:
    struct Concept : PhysicalOperatorConcept
    {
        explicit Concept(OperatorId existingId) : PhysicalOperatorConcept(existingId) { }
        [[nodiscard]] virtual std::shared_ptr<Concept> clone() const = 0;
        [[nodiscard]] virtual std::string toString() const = 0;
    };

    template <IsPhysicalOperator OperatorType>
    struct Model : Concept
    {
        OperatorType data;

        explicit Model(OperatorType d) : Concept(getNextPhysicalOperatorId()), data(std::move(d)) { }

        Model(OperatorType d, OperatorId existingId) : Concept(existingId), data(std::move(d)) { }

        [[nodiscard]] std::shared_ptr<Concept> clone() const override { return std::make_shared<Model>(data, this->id); }

        [[nodiscard]] std::optional<PhysicalOperator> getChild() const override { return data.getChild(); }

        void setChild(PhysicalOperator child) override { data.setChild(child); }

        void setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const override
        {
            data.setup(executionCtx, engine);
        }

        void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override { data.open(executionCtx, recordBuffer); }

        void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override { data.close(executionCtx, recordBuffer); }

        void terminate(ExecutionContext& executionCtx) const override { data.terminate(executionCtx); }

        void execute(ExecutionContext& executionCtx, Record& record) const override { data.execute(executionCtx, record); }

        [[nodiscard]] std::string toString() const override
        {
            const auto nameDemangled = boost::core::demangle(typeid(OperatorType).name());
            return "PhysicalOperator(" + nameDemangled + ")";
        }
    };

    std::shared_ptr<Concept> self;
};

inline std::ostream& operator<<(std::ostream& os, const PhysicalOperator& op)
{
    return os << op.toString();
}

/// The wrapper provides all information of a physical operator needed for correct pipeline construction during query compilation.
/// The wrapper is removed after pipeline creation. Thus, our physical operators only contain information needed for the actual execution.
class PhysicalOperatorWrapper
{
public:
    enum class PipelineLocation : uint8_t
    {
        SCAN, /// pipeline scan
        EMIT, /// pipeline emit
        INTERMEDIATE, /// neither of them, intermediate operator
    };

    PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema);
    PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema, PipelineLocation pipelineLocation);
    PhysicalOperatorWrapper(
        PhysicalOperator physicalOperator,
        Schema inputSchema,
        Schema outputSchema,
        std::optional<OperatorHandlerId> handlerId,
        std::optional<std::shared_ptr<OperatorHandler>> handler,
        PipelineLocation pipelineLocation);
    PhysicalOperatorWrapper(
        PhysicalOperator physicalOperator,
        Schema inputSchema,
        Schema outputSchema,
        std::optional<OperatorHandlerId> handlerId,
        std::optional<std::shared_ptr<OperatorHandler>> handler,
        PipelineLocation pipelineLocation,
        std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children);

    /// for compatibility with free functions requiring getChildren()
    [[nodiscard]] std::vector<std::shared_ptr<PhysicalOperatorWrapper>> getChildren() const;

    /// Returns a string representation of the wrapper
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    [[nodiscard]] const PhysicalOperator& getPhysicalOperator() const;
    [[nodiscard]] const std::optional<Schema>& getInputSchema() const;
    [[nodiscard]] const std::optional<Schema>& getOutputSchema() const;

    void addChild(const std::shared_ptr<PhysicalOperatorWrapper>& child);
    void setChildren(const std::vector<std::shared_ptr<PhysicalOperatorWrapper>>& newChildren);

    [[nodiscard]] const std::optional<std::shared_ptr<OperatorHandler>>& getHandler() const;
    [[nodiscard]] const std::optional<OperatorHandlerId>& getHandlerId() const;
    [[nodiscard]] PipelineLocation getPipelineLocation() const;

private:
    PhysicalOperator physicalOperator;
    std::optional<Schema> inputSchema;
    std::optional<Schema> outputSchema;
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children;

    std::optional<std::shared_ptr<OperatorHandler>> handler;
    std::optional<OperatorHandlerId> handlerId;
    PipelineLocation pipelineLocation;
};
}
FMT_OSTREAM(NES::PhysicalOperator);
