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
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <typeinfo>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Formatter.hpp>
#include <ErrorHandling.hpp>
#include <Util/PlanRenderer.hpp>

namespace NES
{
using namespace Nautilus;
using namespace Nautilus::Interface::MemoryProvider;

struct ExecutionContext;

/// Unique ID generation for physical operators.
namespace
{
inline OperatorId getNextPhysicalOperatorId()
{
    static std::atomic_uint64_t id = INITIAL_OPERATOR_ID.getRawValue();
    return OperatorId(id++);
}
}

/// Concept defining the interface for all physical operators in the query plan.
/// Physical operators represent operations that are executed during query execution.
struct PhysicalOperatorConcept
{
    virtual ~PhysicalOperatorConcept() = default;

    explicit PhysicalOperatorConcept();
    explicit PhysicalOperatorConcept(OperatorId existingId);

    /// Returns the child operator of this operator, if any.
    [[nodiscard]] virtual std::optional<struct PhysicalOperator> getChild() const = 0;
    
    /// Sets the child operator of this operator.
    virtual void setChild(struct PhysicalOperator child) = 0;

    /// Sets up the operator for execution.
    /// This is called once before the operator starts processing records.
    virtual void setup(ExecutionContext& executionCtx) const;

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
    /// @param executionCtx The execution context.
    /// @param record The record to process.
    virtual void execute(ExecutionContext& executionCtx, Record& record) const;

    /// Returns a string representation of the operator.
    [[nodiscard]] virtual std::string toString() const;

    /// Unique identifier for this operator.
    OperatorId id = INVALID_OPERATOR_ID;
};

/// A type-erased wrapper for physical operators.
/// This class provides type erasure for physical operators, allowing them to be stored
/// and manipulated without knowing their concrete type. It uses the PIMPL pattern
/// to store the actual operator implementation.
/// @tparam T The type of the physical operator. Must inherit from PhysicalOperatorConcept.
template<typename T>
concept IsPhysicalOperator = std::is_base_of_v<PhysicalOperatorConcept, std::remove_cv_t<std::remove_reference_t<T>>>;

/// Type-erased physical operator that can be used to process records.
struct PhysicalOperator
{
    /// Constructs a PhysicalOperator from a concrete operator type.
    /// @tparam T The type of the operator. Must satisfy IsPhysicalOperator concept.
    /// @param op The operator to wrap.
    template <IsPhysicalOperator T>
    PhysicalOperator(const T& op) : self(std::make_unique<Model<T>>(op, op.id))
    {
    }

    PhysicalOperator();
    PhysicalOperator(const PhysicalOperator& other);
    PhysicalOperator(PhysicalOperator&&) noexcept;

    PhysicalOperator& operator=(const PhysicalOperator& other);

    /// Returns the child operator of this operator, if any.
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const;

    /// Sets the child operator of this operator.
    void setChild(PhysicalOperator child);

    /// Sets up the operator for execution.
    void setup(ExecutionContext& executionCtx) const;

    /// Opens the operator for processing records.
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// Closes the operator after processing records.
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// Terminates the operator.
    void terminate(ExecutionContext& executionCtx) const;

    /// Executes the operator on the given record.
    void execute(ExecutionContext& executionCtx, Record& record) const;

    /// Returns a string representation of the operator.
    [[nodiscard]] std::string toString() const;

    /// Returns the unique identifier of this operator.
    [[nodiscard]] OperatorId getId() const;

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

private:
    struct Concept : PhysicalOperatorConcept
    {
        explicit Concept(OperatorId existingId) : PhysicalOperatorConcept(existingId) { }
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
    };

    template <typename T>
    struct Model : Concept
    {
        T data;

        explicit Model(T d) : Concept(getNextPhysicalOperatorId()), data(std::move(d)) { }

        Model(T d, OperatorId existingId) : Concept(existingId), data(std::move(d)) { }

        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::make_unique<Model>(data, this->id); }

        [[nodiscard]] std::optional<PhysicalOperator> getChild() const override { return data.getChild(); }

        void setChild(PhysicalOperator child) override { data.setChild(child); }

        void setup(ExecutionContext& executionCtx) const override { data.setup(executionCtx); }

        void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override { data.open(executionCtx, recordBuffer); }

        void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override { data.close(executionCtx, recordBuffer); }

        void terminate(ExecutionContext& executionCtx) const override { data.terminate(executionCtx); }

        void execute(ExecutionContext& executionCtx, Record& record) const override { data.execute(executionCtx, record); }

        [[nodiscard]] std::string toString() const override
        {
            const auto name = typeid(T).name();
            int status = 0;
            std::unique_ptr<char, void (*)(void*)> demangled(abi::__cxa_demangle(name, nullptr, nullptr, &status), std::free);
            const auto nameDemangled = status == 0 ? demangled.get() : name;
            return "PhysicalOperator(" + std::string(nameDemangled) + ")";
        }
    };

    std::unique_ptr<Concept> self;
};

inline std::ostream& operator<<(std::ostream& os, const PhysicalOperator& op)
{
    return os << op.toString();
}

/// Wrapper for the physical operator to store input and output schema after query optimization.
struct PhysicalOperatorWrapper
{
    PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema);

    /// for compatibility with free functions requiring getChildren()
    [[nodiscard]] std::vector<std::shared_ptr<PhysicalOperatorWrapper>> getChildren() const { return children; }
    /// Returns a string representation of the wrapper
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;

    PhysicalOperator physicalOperator;
    std::optional<Schema> inputSchema, outputSchema;
    std::vector<std::shared_ptr<PhysicalOperatorWrapper>> children{};

    std::optional<std::shared_ptr<OperatorHandler>> handler;
    std::optional<OperatorHandlerId> handlerId;

    bool isScan = false;
    bool isEmit = false;
};
}
FMT_OSTREAM(NES::PhysicalOperator);
