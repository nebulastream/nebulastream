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
struct PhysicalOperatorConcept : public OperatorConcept
{
    PhysicalOperatorConcept(bool isPipelineBreaker = false) : isPipelineBreaker(isPipelineBreaker) {}

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

    const bool isPipelineBreaker;
};

/// Wrapper for the physical operator to store input and output schema after query optimization
struct PhysicalOperatorWithSchema
{
    Operator physicalOperator;
    Schema inputSchema;
    Schema outputSchema;
    std::unique_ptr<PhysicalOperatorWithSchema> child;
};

}
