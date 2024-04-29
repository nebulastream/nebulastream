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
#include <Compiler/CompilationCache.hpp>
#include <Compiler/CompilationRequest.hpp>
#include <Compiler/CompilationResult.hpp>
#include <Compiler/Exceptions/CompilerException.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/LanguageCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Util/Logger/Logger.hpp>
#include <future>
#include <utility>

namespace NES::Compiler {

JITCompiler::JITCompiler(std::map<const Language, std::shared_ptr<const LanguageCompiler>> languageCompilers,
                         bool useCompilationCache)
    : languageCompilers(std::move(languageCompilers)), useCompilationCache(useCompilationCache),
      compilationCache(std::make_shared<CompilationCache>()) {}

std::future<CompilationResult> JITCompiler::handleRequest(std::shared_ptr<const CompilationRequest> request) {
    if (request->getSourceCode() == nullptr) {
        throw CompilerException("No source code provided");
    }
    auto language = request->getSourceCode()->getLanguage();
    auto languageCompiler = languageCompilers.find(language);

    if (languageCompiler == languageCompilers.end()) {
        throw CompilerException("No language compiler found for language: " + getLanguageAsString(language));
    }

    auto compiler = languageCompiler->second;
    auto useCache = this->useCompilationCache;
    auto cache = this->compilationCache;
    auto asyncResult = std::async(std::launch::async, [compiler, request, useCache, cache]() {
        auto sourceCode = *request->getSourceCode();
        if (useCache) {
            if (cache->contains(sourceCode)) {
                NES_DEBUG("Reuse existing binary instead of compiling it");
                return cache->get(sourceCode);
            } else {
                auto result = compiler->compile(request);
                cache->insert(sourceCode, result);
                return result;
            }
        } else {
            return compiler->compile(request);
        }
    });
    return asyncResult;
}

std::future<CompilationResult> JITCompiler::compile(std::shared_ptr<const CompilationRequest> request) {
    return handleRequest(request);
}

JITCompiler::~JITCompiler() { NES_DEBUG("~JITCompiler"); }

}// namespace NES::Compiler
