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

#include <TestUtils/AbstractCompilationBackendTest.hpp>

namespace NES::Nautilus {
std::unique_ptr<Nautilus::Backends::Executable>
AbstractCompilationBackendTest::prepare(std::shared_ptr<Nautilus::Tracing::ExecutionTrace> executionTrace,
                                        const CompilationOptions& options,
                                        const DumpHelper& dumpHelper) {
    executionTrace = ssaCreationPhase.apply(std::move(executionTrace));
    NES_DEBUG("{}", executionTrace.get()->toString());
    auto ir = irCreationPhase.apply(executionTrace);
    NES_DEBUG("{}", ir->toString());
    auto param = this->GetParam();
    auto& compiler = Backends::CompilationBackendRegistry::getPlugin(param);
    return compiler->compile(ir, options, dumpHelper);
}

}// namespace NES::Nautilus