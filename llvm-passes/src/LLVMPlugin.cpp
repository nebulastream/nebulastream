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
#include <LLVMPlugin.hpp>

#include <llvm/IR/PassManager.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Passes/PassPlugin.h>
#include <nautilus/llvm-passes/FunctionInliningPass.hpp>

extern "C" LLVM_ATTRIBUTE_WEAK LLVM_ATTRIBUTE_VISIBILITY_DEFAULT PassPluginLibraryInfo llvmGetPassPluginInfo()
{
    return PassPluginLibraryInfo{
        .APIVersion = LLVM_PLUGIN_API_VERSION,
        .PluginName = "NautilusLLVMPlugin",
        .PluginVersion = "0.0.1",
        .RegisterPassBuilderCallbacks =
            [](PassBuilder& passBuilder)
        {
            passBuilder.registerOptimizerLastEPCallback([](ModulePassManager& modulePassManager, auto)
                                                        { modulePassManager.addPass(NautilusInlineRegistrationPass{}); });
        },
    };
}
