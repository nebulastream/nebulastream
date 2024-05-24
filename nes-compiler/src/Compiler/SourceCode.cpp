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
#include <Compiler/SourceCode.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>

namespace NES::Compiler {

SourceCode::SourceCode(Language language, std::string code) : code(std::move(code)), language(std::move(language)) {
    NES_ASSERT(code.empty(), "Code should not be empty");
}

const Language& SourceCode::getLanguage() const { return language; }

const std::string& SourceCode::getCode() const { return code; }
bool SourceCode::operator==(const SourceCode& rhs) const { return code == rhs.code && language == rhs.language; }
bool SourceCode::operator!=(const SourceCode& rhs) const { return !(rhs == *this); }

}// namespace NES::Compiler
