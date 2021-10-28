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
#ifndef NES_INCLUDE_COMPILER_UTIL_CLANG_FORMAT_HPP_
#define NES_INCLUDE_COMPILER_UTIL_CLANG_FORMAT_HPP_
#include <Compiler/Util/File.hpp>
#include <mutex>

namespace NES::Compiler {

/**
 * @brief Utility, which leverages clang format to
 */
class ClangFormat {
  public:
    /**
     * @brief Create a new clang format instance for a specific language
     * @param language
     */
    explicit ClangFormat(std::string language);
    /**
     * @brief Format a particular file with code
     * @param file
     */
    void formatFile(std::shared_ptr<File> file);

  private:
    const std::string language;
    std::mutex clangFormatMutex;
};

}// namespace NES::Compiler

#endif// NES_INCLUDE_COMPILER_UTIL_CLANG_FORMAT_HPP_
