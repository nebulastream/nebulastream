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

#include <QueryCompiler/CodeGenerator/CCodeGenerator/Statements/FunctionCallStatement.hpp>
#include <QueryCompiler/CodeGenerator/CodeGenerator.hpp>
#include <QueryCompiler/GeneratableTypes/GeneratableTypesFactory.hpp>
#include <utility>

namespace NES::QueryCompilation {
CodeGenerator::CodeGenerator() = default;

CodeGenerator::~CodeGenerator() = default;

CompilerTypesFactoryPtr CodeGenerator::getTypeFactory() { return std::make_shared<GeneratableTypesFactory>(); }
FunctionCallStatementPtr CodeGenerator::call(std::string function) { return FunctionCallStatement::create(std::move(function)); }
}// namespace NES::QueryCompilation
