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
#include <Nautilus/Backends/Flounder/FlounderCompilationBackend.hpp>
#include <Nautilus/Backends/Flounder/FlounderExecutable.hpp>
#include <Nautilus/Backends/Flounder/FlounderLoweringProvider.hpp>
#include <Util/Timer.hpp>

namespace NES::Nautilus::Backends::Flounder {

std::unique_ptr<Executable>
FlounderCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions&, const DumpHelper& dumpHelper) {
    Timer timer("CompilationBasedPipelineExecutionEngine");
    timer.start();
    auto lp = Flounder::FlounderLoweringProvider();
    auto executable = lp.lower(ir, dumpHelper);
    timer.snapshot("Flounder generation");
    return std::make_unique<FlounderExecutable>(std::move(executable));
}

[[maybe_unused]] static CompilationBackendRegistry::Add<FlounderCompilationBackend> flounderCompilerBackend("Flounder");

}// namespace NES::Nautilus::Backends::Flounder