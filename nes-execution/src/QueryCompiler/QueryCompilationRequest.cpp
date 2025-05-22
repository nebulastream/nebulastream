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
#include <cstddef>
#include <memory>
#include <utility>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/QueryCompilationRequest.hpp>

namespace NES::QueryCompilation
{

std::shared_ptr<QueryCompilationRequest>
QueryCompilationRequest::create(std::shared_ptr<DecomposedQueryPlan> decomposedQueryPlan, size_t bufferSize)
{
    return std::make_shared<QueryCompilationRequest>(QueryCompilationRequest(std::move(decomposedQueryPlan), bufferSize));
}

QueryCompilationRequest::QueryCompilationRequest(std::shared_ptr<DecomposedQueryPlan> decomposedQueryPlan, size_t bufferSize)
    : decomposedQueryPlan(std::move(decomposedQueryPlan)), debug(false), optimize(false), dumpQueryPlans(false), bufferSize(bufferSize)
{
}

void QueryCompilationRequest::enableDump()
{
    this->dumpQueryPlans = true;
}

void QueryCompilationRequest::enableDebugging()
{
    this->debug = true;
}

void QueryCompilationRequest::enableOptimizations()
{
    this->optimize = true;
}

bool QueryCompilationRequest::isDebugEnabled() const
{
    return debug;
}

bool QueryCompilationRequest::isOptimizeEnabled() const
{
    return optimize;
}

bool QueryCompilationRequest::isDumpEnabled() const
{
    return dumpQueryPlans;
}

std::shared_ptr<DecomposedQueryPlan> QueryCompilationRequest::getDecomposedQueryPlan()
{
    return decomposedQueryPlan;
}

size_t QueryCompilationRequest::getBufferSize() const
{
    return bufferSize;
}

void QueryCompilationRequest::setBufferSize(size_t bufferSize)
{
    this->bufferSize = bufferSize;
}

}
