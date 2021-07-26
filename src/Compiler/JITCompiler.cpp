/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Compiler/JITCompiler.hpp>
#include <Compiler/LanguageCompiler.hpp>
#include <Compiler/SourceCode.hpp>
#include <Util/Logger.hpp>
#include <utility>
#include <future>

namespace NES::Compiler {

JITCompiler::JITCompiler(std::map<const std::string, std::shared_ptr<const LanguageCompiler>> languageCompilers)
    : languageCompilers(std::move(languageCompilers)) {}

std::future<const CompilationResult> JITCompiler::compile(std::unique_ptr<const CompilationRequest> request) const {
    std::promise<const CompilationResult> result;
    auto language = request->getSourceCode()->getLanguage();
    auto languageCompiler = languageCompilers.find(language);

    if (languageCompiler == languageCompilers.end()) {
        NES_FATAL_ERROR("No language compiler found for language: " << language);
    }
    auto asyncResult = std::async(std::launch::async, [&languageCompiler, &request, &result]() {
        auto compilationResult = languageCompiler->second->compile(std::move(request));
        result.set_value(compilationResult);
    });
    return result.get_future();
}

}// namespace NES::Compiler