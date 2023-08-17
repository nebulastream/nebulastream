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

#include <Nautilus/Backends/Experimental/Vectorization/KernelCompiler.hpp>

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Experimental/Vectorization/VectorizableOperator.hpp>
#include <Execution/Operators/Operator.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/CodeGen/CodeGenerator.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>

namespace NES::Nautilus::Backends {

std::unique_ptr<KernelExecutable> KernelCompiler::compile(const std::shared_ptr<Runtime::Execution::Operators::Operator>& nautilusOperator, const CompilationOptions& options, const DumpHelper& dumpHelper) {
    auto trace = createTrace(nautilusOperator);
    auto ir = createIR(trace);
    auto codeGenerator = createCodeGenerator(ir);
    return createExecutable(std::move(codeGenerator), options, dumpHelper);
}

} // namespace NES::Nautilus::Backends
