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

#ifndef COMPILEWITHCLANG_COMPILERINVOKER_H
#define COMPILEWITHCLANG_COMPILERINVOKER_H

#include <Compiler/Util/ExecutablePath.hpp>
#include <boost/outcome.hpp>
#include <string>
#include <vector>

struct CompilerError {
    std::string message;
};

using Result = boost::outcome_v2::result<std::string, CompilerError>;

class CompilerInvoker {
  public:
    explicit CompilerInvoker(NES::Compiler::ExecutablePath::RuntimePathConfig pathConfig) : pathConfig(std::move(pathConfig)) {}

    Result compileToObject(const std::string& sourceCode, const std::string& outputPath) const;
    Result compile(const std::string& sourceCode) const;

  private:
    NES::Compiler::ExecutablePath::RuntimePathConfig pathConfig;
};

#endif//COMPILEWITHCLANG_COMPILERINVOKER_H