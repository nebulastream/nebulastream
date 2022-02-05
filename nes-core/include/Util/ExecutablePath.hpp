#ifndef NES_NES_CORE_INCLUDE_UTIL_EXECUTABLEPATH_HPP_
#define NES_NES_CORE_INCLUDE_UTIL_EXECUTABLEPATH_HPP_

#include <filesystem>
namespace NES::ExecutablePath {

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

}// namespace NES::ExecutablePath

#endif//NES_NES_CORE_INCLUDE_UTIL_EXECUTABLEPATH_HPP_
