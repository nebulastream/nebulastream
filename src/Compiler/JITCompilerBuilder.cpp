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
#include <Compiler/JITCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Compiler/LanguageCompiler.hpp>
#include <Util/Logger.hpp>
namespace NES::Compiler {

JITCompilerBuilder&
JITCompilerBuilder::registerLanguageCompiler(const std::shared_ptr<const LanguageCompiler>& languageCompiler) {
    NES_ASSERT(languageCompiler, "Language compiler should not be null.");
    NES_ASSERT(!languageCompiler->getLanguage().empty(), "Invalid language provided.");
    NES_ASSERT(languageCompilers.find(languageCompiler->getLanguage()) == languageCompilers.end(),
               "Compiler for " << languageCompiler->getLanguage() << " was already registered");
    this->languageCompilers[languageCompiler->getLanguage()] = languageCompiler;
    return *this;
}

std::shared_ptr<JITCompiler> JITCompilerBuilder::build() { return std::make_shared<JITCompiler>(languageCompilers); }

}// namespace NES::Compiler