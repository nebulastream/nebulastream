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
#include <cstdint>
#include <memory>
#include <utility>
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <Util/Timer.hpp>
#include <ExecutableQueryPlan.hpp>

namespace NES::QueryCompilation
{

QueryCompilationResultPtr QueryCompilationResult::create(std::unique_ptr<Runtime::Execution::ExecutableQueryPlan> qep, Timer<>&& timer)
{
    return std::make_shared<QueryCompilationResult>(QueryCompilationResult(std::move(qep), std::move(timer)));
}
QueryCompilationResult::QueryCompilationResult(std::unique_ptr<Runtime::Execution::ExecutableQueryPlan> executableQueryPlan, Timer<> timer)
    : executableQueryPlan(std::move(executableQueryPlan)), timer(std::move(timer))
{
}

std::unique_ptr<Runtime::Execution::ExecutableQueryPlan> QueryCompilationResult::takeExecutableQueryPlan()
{
    return std::move(executableQueryPlan);
}

uint64_t QueryCompilationResult::getCompilationTimeMilli() const
{
    return timer.getRuntime();
}

}
