/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <QueryCompiler/QueryCompilationResult.hpp>
#include <Util/Timer.hpp>
#include <exception>
#include <utility>

namespace NES::QueryCompilation {

QueryCompilationResultPtr QueryCompilationResult::create(Runtime::Execution::ExecutableQueryPlanPtr qep, Timer<>& timer) {
    return std::make_shared<QueryCompilationResult>(QueryCompilationResult(std::move(qep), std::move(timer)));
}
QueryCompilationResult::QueryCompilationResult(Runtime::Execution::ExecutableQueryPlanPtr executableQueryPlan, Timer<> timer)
    : executableQueryPlan(executableQueryPlan), timer(timer) {}
QueryCompilationResult::QueryCompilationResult(std::exception_ptr exception) : exception(exception) {}

QueryCompilationResultPtr QueryCompilationResult::create(std::exception_ptr exception) {
    return std::make_shared<QueryCompilationResult>(QueryCompilationResult(std::move(exception)));
}

Runtime::Execution::ExecutableQueryPlanPtr QueryCompilationResult::getExecutableQueryPlan() {
    if (hasError()) {
        std::rethrow_exception(exception.value());
    }
    return executableQueryPlan.value();
}

uint64_t QueryCompilationResult::getCompilationTime() const { return timer->getRuntime(); }

bool QueryCompilationResult::hasError() { return exception.has_value(); }

std::exception_ptr QueryCompilationResult::getError() { return exception.value(); }

}// namespace NES::QueryCompilation