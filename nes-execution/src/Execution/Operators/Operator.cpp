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
#include <Execution/Operators/Operator.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators
{

void Operator::setup(ExecutionContext& executionCtx) const
{
    if (hasChild())
    {
        child->setup(executionCtx);
    }
}

void Operator::open(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    if (hasChild())
    {
        child->open(executionCtx, rb);
    }
}

void Operator::close(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    if (hasChild())
    {
        child->close(executionCtx, rb);
    }
}

bool Operator::hasChild() const
{
    return child != nullptr;
}

void Operator::setChild(std::shared_ptr<Operators::ExecutableOperator> child)
{
    PRECONDITION(!hasChild(), "This operator already has a child operator");
    this->child = std::move(child);
}

void Operator::terminate(ExecutionContext& executionCtx) const
{
    if (hasChild())
    {
        child->terminate(executionCtx);
    }
}

Operator::~Operator() = default;

}
