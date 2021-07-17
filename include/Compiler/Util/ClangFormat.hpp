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
#ifndef NES_INCLUDE_COMPILER_UTIL_FORMATER_HPP_
#define NES_INCLUDE_COMPILER_UTIL_FORMATER_HPP_
#include <Compiler/File.hpp>
#include <Compiler/Language.hpp>
#include <mutex>

namespace NES ::Compiler {

class ClangFormat {
  public:
    explicit ClangFormat(Language language);
    void formatFile(std::shared_ptr<File> file);

  private:
    std::string getFileType() const;
    Language language;
    std::mutex clangFormatMutex;

};

}// namespace NES::Compiler

#endif//NES_INCLUDE_COMPILER_UTIL_FORMATER_HPP_
