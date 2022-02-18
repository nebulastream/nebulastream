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
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/LanguageCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Util/Logger.hpp>
#include <future>
#include <utility>

namespace NES::Compiler {

JITCompiler::JITCompiler(std::map<const std::string, std::shared_ptr<const LanguageCompiler>> languageCompilers)
    : languageCompilers(std::move(languageCompilers)) {}

CompilationResult JITCompiler::handleRequest(std::shared_ptr<const CompilationRequest> request) const {
    if (request->getSourceCode() == nullptr) {
        throw CompilerException("No source code provided");
    }
    auto language = request->getSourceCode()->getLanguage();
    auto languageCompiler = languageCompilers.find(language);

    if (languageCompiler == languageCompilers.end()) {
        throw CompilerException("No language compiler found for language: " + language);
    }
    return languageCompiler->second->compile(request);
}

std::future<CompilationResult> JITCompiler::compile(std::shared_ptr<const CompilationRequest> request) const {
    auto asyncResult = std::async(std::launch::async, [this, request]() {
        return this->handleRequest(request);
    });
    return asyncResult;
}

}// namespace NES::Compiler