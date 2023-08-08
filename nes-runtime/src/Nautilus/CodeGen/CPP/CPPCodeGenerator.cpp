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

#include <Nautilus/CodeGen/CPP/CPPCodeGenerator.hpp>

#include <sstream>

namespace NES::Nautilus::CodeGen::CPP {

std::string CPPCodeGenerator::toString() const {
    std::stringstream code;
    for (const auto& file : includes) {
        code << "#include " << file << "\n";
    }
    for (const auto& decl : declarations) {
        code << decl << ";\n";
    }
    for (const auto& fn : functions) {
        code << fn->toString() << "\n";
    }
    return code.str();
}

void CPPCodeGenerator::addInclude(const std::string& file) {
    includes.insert(file);
}

void CPPCodeGenerator::addDeclaration(const std::string& declaration) {
    declarations.insert(declaration);
}

void CPPCodeGenerator::addFunction(const std::shared_ptr<Function>& fn) {
    functions.push_back(fn);
    declarations.insert(fn->getSignature());
}

} // namespace NES::Nautilus::CodeGen::CPP
