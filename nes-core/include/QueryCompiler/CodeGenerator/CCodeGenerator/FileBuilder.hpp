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

#ifndef NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_FILE_BUILDER_HPP_
#define NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_FILE_BUILDER_HPP_

#include <QueryCompiler/CodeGenerator/CodeGeneratorForwardRef.hpp>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace NES::QueryCompilation {

class CodeFile {
  public:
    std::string code;
};

class FileBuilder {
  private:
    std::stringstream declarations;

  public:
    static FileBuilder create(const std::string& file_name, const std::unordered_set<std::string>& headers = {});
    FileBuilder& addDeclaration(DeclarationPtr const&);
    CodeFile build();
};

}// namespace NES::QueryCompilation
#endif// NES_INCLUDE_QUERY_COMPILER_CODE_GENERATOR_C_CODE_GENERATOR_FILE_BUILDER_HPP_
