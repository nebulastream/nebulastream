#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/Language.hpp>
#include <Compiler/LanguageCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Util/Logger.hpp>
#include <utility>

namespace NES::Compiler {

JITCompiler::JITCompiler(std::map<const Language, std::shared_ptr<const LanguageCompiler>> languageCompilers)
    : languageCompilers(std::move(languageCompilers)) {}

std::future<const CompilationResult> JITCompiler::compile(std::unique_ptr<const CompilationRequest> request) const {
    std::promise<const CompilationResult> result;
    auto language = request->getSourceCode()->getLanguage();
    auto languageCompiler = languageCompilers.find(language);

    if (languageCompiler == languageCompilers.end()) {
        NES_FATAL_ERROR("No language compiler found for language: " << language);
    }

    auto compilationResult = languageCompiler->second->compile(std::move(request));
   // result.set_value(compilationResult);
    return result.get_future();
}

}// namespace NES::Compiler