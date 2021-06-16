#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/CPPCompiler/CPPCompilerFlags.hpp>

namespace NES::Compiler{

std::shared_ptr<LanguageCompiler> CPPCompiler::create() {
    return std::make_shared<CPPCompiler>();
}

Language CPPCompiler::getLanguage() const { return CPP; }

std::unique_ptr<const CompilationResult> CPPCompiler::compile(std::unique_ptr<const CompilationRequest> request) const {

    auto compilationFlags = CPPCompilerFlags::createDefaultCompilerFlags();
    if(request->enableOptimizations()){
        compilationFlags.enableOptimizationFlags();
    }
    if(request->enableDebugging()){
        compilationFlags.enableDebugFlags();
    }


    return std::make_unique<CompilationResult>();
}


CPPCompiler::~CPPCompiler() = default;

}