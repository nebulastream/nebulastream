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
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Plans/Operator.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES
{
using namespace Nautilus;
using namespace Nautilus::Interface::MemoryProvider;
struct ExecutionContext;

/// Each operator can implement setup, open, close, execute, and terminate.
struct PhysicalOperator : public virtual Operator
{
    PhysicalOperator(bool isPipelineBreaker = false) : isPipelineBreaker(isPipelineBreaker) {}

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

    void setChild(std::unique_ptr<PhysicalOperator> op) {
        children.clear();
        children.push_back(std::move(op));
    }
    const bool isPipelineBreaker;

private:

    /// raw pointer is only exposed in PhysicalOperator
    [[nodiscard]] const PhysicalOperator* child() const {
        INVARIANT(children.size() == 1, "Must have exactly one child but got {}", children.size());
        return dynamic_cast<PhysicalOperator*>(children.front().get());
    }
    /// Hide the base class 'children' from subclasses of PhysicalOperator
    /// We expect PhysicalOperators to have exactly one child used with the member function child().
    using Operator::children;
};

/// Wrapper for the physical operator to store input and output schema after query optimization
struct PhysicalOperatorWithSchema
{
    Schema inputSchema;
    Schema outputSchema;
    std::unique_ptr<PhysicalOperatorWithSchema> child;
};

}
