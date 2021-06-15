#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/LanguageCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Util/Logger.hpp>

namespace NES::Compiler {

JITCompiler::JITCompiler(const std::vector<std::unique_ptr<LanguageCompiler>> languageCompilers) {
    for (auto& languageCompiler : languageCompilers) {
        NES_DEBUG("Register compiler for language: " << languageCompiler->getLanguage());
        languageCompilerMap[languageCompiler->getLanguage()] = languageCompiler;
    }
}

const std::future<CompilationResult> JITCompiler::compile(std::unique_ptr<CompilationRequest> request) const {
    std::promise<CompilationResult> result;
    auto language = request->getSourceCode()->getLanguage();
    auto languageCompiler = languageCompilerMap.find(language);

    if (languageCompiler == languageCompilerMap.end()) {
        NES_FATAL_ERROR("No language compiler found for language: " << language);
    }

    auto compilationResult = languageCompiler->second->compile(std::move(request));
    result.set_value(compilationResult);
    return result.get_future();
}

}// namespace NES::Compiler