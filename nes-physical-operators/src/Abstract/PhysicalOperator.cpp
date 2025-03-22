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

#include <Abstract/PhysicalOperator.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

void PhysicalOperatorConcept::setup(ExecutionContext& executionCtx) const
{
    if (getChild())
    {
        getChild().value().setup(executionCtx);
    }
}

void PhysicalOperatorConcept::open(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    if (getChild())
    {
        getChild().value().open(executionCtx, rb);
    }
}

void PhysicalOperatorConcept::close(ExecutionContext& executionCtx, RecordBuffer& rb) const
{
    if (getChild())
    {
        getChild().value().close(executionCtx, rb);
    }
}

void PhysicalOperatorConcept::terminate(ExecutionContext& executionCtx) const
{
    if (getChild())
    {
        getChild().value().terminate(executionCtx);
    }
}

void PhysicalOperatorConcept::execute(NES::ExecutionContext& executionCtx, NES::Nautilus::Record& record) const
{
    if (getChild())
    {
        getChild().value().execute(executionCtx, record);
    }
}

}
