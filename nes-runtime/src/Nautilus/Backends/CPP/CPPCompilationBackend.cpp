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
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/DynamicObject.hpp>
#include <Compiler/SourceCode.hpp>
#include <Nautilus/Backends/CPP/CPPCompilationBackend.hpp>
#include <Nautilus/Backends/CPP/CPPExecutable.hpp>
#include <Nautilus/Backends/CPP/CPPLoweringProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Timer.hpp>

namespace NES::Nautilus::Backends::CPP {

// this makes nes crash if the logger singleton is destroyed before BCInterpreterBackend object as its dtor prints
[[maybe_unused]] static CompilationBackendRegistry::Add<CPPCompilationBackend> cppCompilationBackend("CPPCompiler");

std::unique_ptr<Executable>
CPPCompilationBackend::compile(std::shared_ptr<IR::IRGraph> ir, const CompilationOptions&, const DumpHelper&) {
    auto timer = Timer<>("CompilationBasedPipelineExecutionEngine");
    timer.start();

    auto code = CPPLoweringProvider().lower(ir);
    NES_INFO(code);
    timer.snapshot("CPPCodeGeneration");

    auto compiler = Compiler::CPPCompiler::create();
    auto sourceCode = std::make_unique<Compiler::SourceCode>("cpp", code);
    auto request = Compiler::CompilationRequest::create(std::move(sourceCode), "cppQuery", false, false, true, true);
    auto res = compiler->compile(request);
    return std::make_unique<CPPExecutable>(res.getDynamicObject());
}

}// namespace NES::Nautilus::Backends::CPP