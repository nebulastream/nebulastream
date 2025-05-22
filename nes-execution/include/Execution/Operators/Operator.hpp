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
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>

namespace NES::Runtime::Execution
{
struct ExecutionContext;
}

namespace NES::Runtime::Execution::Operators
{
using namespace Nautilus;
class ExecutableOperator;

/**
 * @brief Base operator for all specific operators.
 * Each operator can implement setup, open, close, and terminate.
 */
class Operator
{
public:
    /**
     * @brief Setup initializes this operator for execution.
     * Operators can implement this class to initialize some state that exists over the whole life time of this operator.
     * @param executionCtx the RuntimeExecutionContext
     */
    virtual void setup(ExecutionContext& executionCtx) const;

    /**
     * @brief Open is called for each record buffer and is used to initializes execution local state.
     * @param recordBuffer
     */
    virtual void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /**
     * @brief Close is called for each record buffer and clears execution local state.
     * @param executionCtx
     * @param recordBuffer
     */
    virtual void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const;

    /**
     * @brief Terminates the operator and clears all operator state.
     * @param executionCtx the RuntimeExecutionContext
     */
    virtual void terminate(ExecutionContext& executionCtx) const;

    /**
     * @return Returns true if the operator has a child.
     */
    bool hasChild() const;

    /**
     * @brief Sets a child operator to this operator.
     * @param child
     */
    void setChild(std::shared_ptr<ExecutableOperator> child);

    virtual ~Operator();

protected:
    std::shared_ptr<ExecutableOperator> child;
};

}
