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

#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CPPCompiler/CUDAPlatform.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/SourceCode.hpp>
#include <Nautilus/Backends/CPP/CPPCompilationBackend.hpp>
#include <Nautilus/Backends/CPP/CPPExecutable.hpp>
#include <Nautilus/Backends/CPP/CPPLoweringProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>

namespace NES::Nautilus::Backends::CPP {

[[maybe_unused]] static CompilationBackendRegistry::Add<CPPCompilationBackend> cppCompilationBackend("CPPCompiler");

std::unique_ptr<Executable>
CPPCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions& options, const DumpHelper& dumpHelper) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    auto code = CPPLoweringProvider::lower(ir);
    dumpHelper.dump("code.cpp", code);

    timer.snapshot("CPPCodeGeneration");

    auto compiler = Compiler::CPPCompiler::create();
    auto sourceCode = std::make_unique<Compiler::SourceCode>(Compiler::Language::CPP, code);

    std::vector<std::shared_ptr<Compiler::ExternalAPI>> externalApis;
    if (options.usingCUDA()) {
        externalApis.push_back(std::make_shared<Compiler::CUDAPlatform>(options.getCUDASdkPath()));
    }

    auto request = Compiler::CompilationRequest::create(std::move(sourceCode),
                                                        "cppQuery",
                                                        options.isDebug(),
                                                        false,
                                                        options.isOptimize(),
                                                        options.isDebug(),
                                                        externalApis);
    auto res = compiler->compile(request);
    timer.snapshot("CCPCompilation");
    return std::make_unique<CPPExecutable>(res.getDynamicObject());
}

}// namespace NES::Nautilus::Backends::CPP