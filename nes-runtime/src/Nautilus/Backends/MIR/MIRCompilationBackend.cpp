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
#include <Nautilus/Backends/MIR/MIRCompilationBackend.hpp>
#include <Nautilus/Backends/MIR/MIRExecutable.hpp>
#include <Nautilus/Backends/MIR/MIRLoweringProvider.hpp>
#include <Util/Timer.hpp>
#include <mir-gen.h>
#include <mir.h>

namespace NES::Nautilus::Backends::MIR {

std::unique_ptr<Executable>
MIRCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions&, const DumpHelper& dumpHelper) {
    Timer timer("MIR Compiler");
    timer.start();
    MIR_module_t m;
    if (ctx == nullptr) {
        ctx = MIR_init();
        MIR_gen_init(ctx, 1);
        MIR_link(ctx, MIR_set_lazy_gen_interface, NULL);
    }
    auto lp = MIR::MIRLoweringProvider();
    auto func = lp.lower(ir, dumpHelper, ctx, &m);
    timer.snapshot("MIR generation");
    //MIR_output(ctx, stderr);
    MIR_load_module(ctx, m);
    timer.snapshot("MIR loading");
    MIR_gen_set_optimize_level(ctx, 0, 1);
    // MIR_gen_set_debug_file(ctx, 0, stderr);
    // MIR_gen_set_debug_level(ctx, 0 , 1);
    MIR_link(ctx, MIR_set_lazy_gen_interface, NULL);
    timer.snapshot("MIR linking");
    auto fun_addr = MIR_gen(ctx, 0, func);
    timer.snapshot("MIR backend generation");
    NES_INFO(timer);
    return std::make_unique<MIRExecutable>(ctx, fun_addr);
}

[[maybe_unused]] static CompilationBackendRegistry::Add<MIRCompilationBackend> MIRCompilerBackend("MIR");

}// namespace NES::Nautilus::Backends::MIR