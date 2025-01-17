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

#include <memory>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Util/Logger/Logger.hpp>
#include "Nautilus/Interface/Record.hpp"
#include "Nautilus/Interface/RecordBuffer.hpp"
namespace NES::Runtime::Execution::Operators
{

void ExecutableOperator::setup(ExecutionContext& executionCtx) const
{
    if (hasChild())
    {
        child->setup(executionCtx);
    }
}

void ExecutableOperator::open(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    if (hasChild())
    {
        child->open(executionCtx, rb);
    }
}

void ExecutableOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// As a default, we are calling the child operator to execute the record.
    if (hasChild())
    {
        child->execute(ctx, record);
    }
}

void ExecutableOperator::close(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    if (hasChild())
    {
        child->close(executionCtx, rb);
    }
}

bool ExecutableOperator::hasChild() const
{
    return child != nullptr;
}

void ExecutableOperator::setChild(std::shared_ptr<ExecutableOperator> child) const
{
    if (hasChild())
    {
        NES_THROW_RUNTIME_ERROR("This ExecutableOperator already has a child ExecutableOperator");
    }
    this->child = std::move(child);
}

void ExecutableOperator::terminate(ExecutionContext& executionCtx) const
{
    if (hasChild())
    {
        child->terminate(executionCtx);
    }
}

ExecutableOperator::~ExecutableOperator() = default;

}
