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
#ifndef NES_COMPILER_INCLUDE_COMPILER_UTIL_EXECUTABLEPATH_HPP_
#define NES_COMPILER_INCLUDE_COMPILER_UTIL_EXECUTABLEPATH_HPP_

#include <filesystem>
#include <ostream>
#include <vector>
namespace NES::Compiler::ExecutablePath {

/**
 * @brief This class holds information about the runtime path of different nes components.
 */
class RuntimePathConfig {
  public:
    std::string clangBinaryPath;
    std::vector<std::string> includePaths;
    std::vector<std::string> libPaths;
    std::vector<std::string> libs;
    friend std::ostream& operator<<(std::ostream& os, const RuntimePathConfig& config);
};

bool isInInstallDir();

[[maybe_unused]] RuntimePathConfig loadRuntimePathConfig();

/**
 * @brief Gets the path of the current executable.
 * @return std::filesystem::path
 */
std::filesystem::path getExecutablePath();

/**
 * @brief Gets the path to the public includes.
 * @return std::filesystem::path
 */
std::filesystem::path getPublicIncludes();

/**
 * @brief Gets the path to the nes lib
 * @return std::filesystem::path
 */
std::filesystem::path getLibPath(std::string libName);

/**
 * @brief Gets the path to the nes clang executable
 * @return std::filesystem::path
 */
[[maybe_unused]] std::filesystem::path getClangPath();

}// namespace NES::Compiler::ExecutablePath

#endif // NES_COMPILER_INCLUDE_COMPILER_UTIL_EXECUTABLEPATH_HPP_
