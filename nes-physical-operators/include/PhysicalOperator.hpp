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
#include <Plans/Operator.hpp>
#include <Util/Common.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

/// Each operator can implement setup, open, close, execute, and terminate.
class PhysicalOperator
{
public:
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
    virtual void execute(ExecutionContext& ctx, Record& record) const = 0;

    virtual ~PhysicalOperator();

    mutable std::shared_ptr<const PhysicalOperator> child;
};

}
