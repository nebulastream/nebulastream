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

#include <Nautilus/Backends/BCInterpreter/BCInterpreter.hpp>
#include <Nautilus/Backends/BCInterpreter/BCInterpreterBackend.hpp>
#include <Nautilus/Backends/BCInterpreter/BCLoweringProvider.hpp>
#include <Nautilus/Backends/BCInterpreter/ByteCode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>

namespace NES::Nautilus::Backends::BC {

// this makes nes crash if the logger singleton is destroyed before BCInterpreterBackend object as its dtor prints
[[maybe_unused]] static CompilationBackendRegistry::Add<BCInterpreterBackend> bcInterpreterBackend("BCInterpreter");

std::unique_ptr<Executable>
BCInterpreterBackend::compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions&, const DumpHelper& dumpHelper) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    auto result = BCLoweringProvider().lower(ir);

    timer.snapshot("ByteCodeGeneration");

    auto code = std::get<0>(result);
    dumpHelper.dump("3. ByteCode.bc", code.toString());
    return std::make_unique<BCInterpreter>(std::get<0>(result), std::get<1>(result));
}

}// namespace NES::Nautilus::Backends::BC