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

#include <memory>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
using namespace Nautilus;
using namespace Nautilus::Interface::MemoryProvider;
struct ExecutionContext;

/// Each operator can implement setup, open, close, execute, and terminate.
struct PhysicalOperatorConcept
{
    virtual ~PhysicalOperatorConcept() = default;

    virtual std::optional<struct PhysicalOperator> getChild() const = 0;
    virtual void setChild(struct PhysicalOperator child) = 0;

    /// @brief Setup initializes this operator for execution.
    /// Operators can implement this class to initialize some state that exists over the whole lifetime of this operator.
    virtual void setup(ExecutionContext& executionCtx) const;

    /// @brief Open is called for each record buffer and is used to initializes execution local state.
    virtual void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// @brief Close is called for each record buffer and clears execution local state.
    virtual void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /// @brief Terminates the operator and clears all operator state.
    virtual void terminate(ExecutionContext& executionCtx) const;

    /// @brief This method is called by the upstream operator (parent) and passes one record for execution.
    /// @param ctx the execution context that allows accesses to local and global state.
    /// @param record the record that should be processed.
    virtual void execute(ExecutionContext&, Record&) const;

    virtual std::string toString() const { return "PhysicalOperatorConcept"; }
};

struct PhysicalOperator
{
public:
    template <typename T>
    PhysicalOperator(const T& op) : self(std::make_unique<Model<T>>(op))
    {
    }

    PhysicalOperator(const PhysicalOperator& other) : self(other.self->clone()) { }

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

    PhysicalOperator(PhysicalOperator&&) noexcept = default;

    PhysicalOperator& operator=(const PhysicalOperator& other)
    {
        if (this != &other)
        {
            self = other.self->clone();
        }
        return *this;
    }

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const { return self->getChild(); }

    void setChild(PhysicalOperator child) { self->setChild(child); }

    void setup(ExecutionContext& executionCtx) const { self->setup(executionCtx); }

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const { self->open(executionCtx, recordBuffer); }

    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const { self->close(executionCtx, recordBuffer); }

    void terminate(ExecutionContext& executionCtx) const { self->terminate(executionCtx); }

    void execute(ExecutionContext& executionCtx, Record& record) const { self->execute(executionCtx, record); }


    std::string toString() const { return self->toString(); }

    struct Concept : PhysicalOperatorConcept
    {
        [[nodiscard]] virtual std::unique_ptr<Concept> clone() const = 0;
    };

    template <typename T>
    struct Model : Concept
    {
        T data;
        explicit Model(T d) : data(std::move(d)) { }

        [[nodiscard]] std::unique_ptr<Concept> clone() const override { return std::unique_ptr<Concept>(new Model<T>(data)); }

        [[nodiscard]] std::optional<PhysicalOperator> getChild() const override { return data.getChild(); }

        void setChild(PhysicalOperator child) override { data.setChild(child); }

        void setup(ExecutionContext& executionCtx) const override { data.setup(executionCtx); }

        void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override { data.open(executionCtx, recordBuffer); }

        void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override { data.close(executionCtx, recordBuffer); }

        void terminate(ExecutionContext& executionCtx) const override { data.terminate(executionCtx); }

        void execute(ExecutionContext& executionCtx, Record& record) const override { data.execute(executionCtx, record); }

        [[nodiscard]] bool equals(const Concept& other) const override { data.terminate(executionCtx); }

        void execute(ExecutionContext& executionCtx, Record& record) const override { data.execute(executionCtx, record); }
    };

    std::string toString() const override { return "PhysicalOperator(" + std::string(typeid(T).name()) + ")"; }

    std::unique_ptr<Concept> self;
};

/// Wrapper for the physical operator to store input and output schema after query optimization
struct PhysicalOperatorWrapper
{
    PhysicalOperatorWrapper(PhysicalOperator physicalOperator, Schema inputSchema, Schema outputSchema)
        : physicalOperator(physicalOperator), inputSchema(inputSchema), outputSchema(outputSchema) {};

    PhysicalOperator physicalOperator;
    std::optional<Schema> inputSchema, outputSchema;
    std::vector<std::unique_ptr<PhysicalOperatorWrapper>> children;
};
}

FMT_OSTREAM(NES::PhysicalOperator);
