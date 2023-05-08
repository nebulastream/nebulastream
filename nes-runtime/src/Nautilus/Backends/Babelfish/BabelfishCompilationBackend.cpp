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

#include <Nautilus/Backends/Babelfish/BabelfishCompilationBackend.hpp>
#include <Nautilus/Backends/Babelfish/BabelfishLoweringProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>

namespace NES::Nautilus::Backends::Babelfish {

[[maybe_unused]] static CompilationBackendRegistry::Add<BabelfishCompilationBackend> babelfishBackend("BabelfishCompiler");

std::unique_ptr<Executable>
BabelfishCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions&, const DumpHelper& dumpHelper) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    auto code = BabelfishLoweringProvider::lower(ir);
    dumpHelper.dump("babelfish.json", code);

    timer.snapshot("BabelfishIRGeneration");

    return nullptr;
}

}// namespace NES::Nautilus::Backends::Babelfish