#include <Compiler/JITCompilerBuilder.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/LanguageCompiler.hpp>
namespace NES::Compiler{

JITCompilerBuilder::JITCompilerBuilder() = default;

void JITCompilerBuilder::registerLanguageCompiler(const std::shared_ptr<const LanguageCompiler>& languageCompiler) {
    this->languageCompilers[languageCompiler->getLanguage()] = languageCompiler;
}

std::shared_ptr<JITCompiler> JITCompilerBuilder::build() {
    return std::make_shared<JITCompiler>(languageCompilers);
}

}