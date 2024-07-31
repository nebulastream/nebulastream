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

#include <optional>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Util/Timer.hpp>

namespace NES::QueryCompilation
{

/**
 * @brief Provides the query compilation results.
 * Query compilation can succeed, in this case the result contains a ExecutableQueryPlan pointer.
 * If query compilation fails, the result contains the error and hasError() return true.
 */
class QueryCompilationResult
{
public:
    static QueryCompilationResultPtr create(Runtime::Execution::ExecutableQueryPlanPtr qep, Timer<>&& timer);
    /**
     * @brief Returns the query execution plan if hasError() == false.
     * @throws QueryCompilationException if hasError() == true.
     * @return NewExecutableQueryPlanPtr
     */
    Runtime::Execution::ExecutableQueryPlanPtr getExecutableQueryPlan();

    /**
    * @brief Returns the compilation time
    * @return compilation time
    */
    [[nodiscard]] uint64_t getCompilationTime() const;

private:
    explicit QueryCompilationResult(Runtime::Execution::ExecutableQueryPlanPtr executableQueryPlan, Timer<> timer);
    Runtime::Execution::ExecutableQueryPlanPtr executableQueryPlan;
    Timer<std::chrono::nanoseconds, std::milli, double, std::chrono::high_resolution_clock> timer;
};
} /// namespace NES::QueryCompilation
