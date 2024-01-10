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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_UTIL_COMPILATIONOPTIONS_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_UTIL_COMPILATIONOPTIONS_HPP_
#include <cstdint>
#include <string>
namespace NES::Nautilus {

/**
 * @brief Defines a set of compilation options for nautilus compilation backends.
 */
class CompilationOptions {
  public:
    CompilationOptions() = default;
    /**
     * @brief Retrieves the identifier for this compilation options instance.
     * @return A const reference to the identifier string.
     */
    const std::string getIdentifier() const;

    /**
     * @brief Sets the identifier for this compilation options instance.
     * @param identifier The identifier string to set.
     */
    void setIdentifier(const std::string& identifier);

    /**
    * @brief Retrieves the dump output path for this compilation options instance.
     * @return A const reference to the dump output path string.
     */
    const std::string getDumpOutputPath() const;

    /**
     * @brief Sets the dump output path for this compilation options instance.
     * @param dumpOutputPath The dump output path string to set.
     */
    void setDumpOutputPath(const std::string& dumpOutputPath);

    /**
     * @brief Checks if the dump to file option is enabled.
     * @return True if the dump to file option is enabled, false otherwise.
     */
    bool isDumpToFile() const;

    /**
     * @brief Sets the dump to file option.
     * @param dumpToFile The value to set the dump to file option to.
     */
    void setDumpToFile(bool dumpToFile);

    /**
     * @brief Checks if the dump to console option is enabled.
     * @return True if the dump to console option is enabled, false otherwise.
     */
    bool isDumpToConsole() const;

    /**
     * @brief Sets the dump to console option.
     * @param dumpToConsole The value to set the dump to console option to.
     */
    void setDumpToConsole(bool dumpToConsole);

    /**
     * @brief Checks if the optimize option is enabled.
     * @return True if the optimize option is enabled, false otherwise.
     */
    bool isOptimize() const;

    /**
     * @brief Sets the optimize option.
     * @param optimize The value to set the optimize option to.
     */
    void setOptimize(bool optimize);

    /**
     * @brief Checks if the debug option is enabled.
     * @return True if the debug option is enabled, false otherwise.
     */
    bool isDebug() const;

    /**
     * @brief Sets the debug option.
     * @param debug The value to set the debug option to.
     */
    void setDebug(bool debug);

    /**
     * @brief Checks if the proxy inlining option is enabled.
     * @return True if the proxy inlining option is enabled, false otherwise.
     */
    bool isProxyInlining() const;

    /**
     * @brief Sets the proxy inlining option.
     * @param proxyInlining: The value to set the proxyInlining option to.
     */
    void setProxyInlining(const bool proxyInlining);

    /**
     * @brief Get the proxy inlining input path.
     */
    const std::string getProxyInliningInputPath() const;

    /**
     * @brief set the optimization level used for compilation.
     */
    void setOptimizationLevel(const uint8_t optimizationLevel);

    /**
     * @brief get the optimization level used for compilation.
     */
    uint8_t getOptimizationLevel() const;

    /**
     * @brief Enable/disable the inclusion of CUDA.
     */
    void useCUDA(bool cuda);

    /**
     * @brief Indicate if we are using CUDA.
     */
    bool usingCUDA() const;

    /**
     * @brief Set the path to the CUDA SDK
     * @param cudaSdkPath the CUDA SDK path
     */
    void setCUDASdkPath(const std::string& cudaSdkPath);

    /**
     * @brief Get the path to the CUDA SDK
     */
    const std::string getCUDASdkPath() const;

  private:
    std::string identifier;
    std::string dumpOutputPath;
    std::string proxyInliningInputPath;
    std::string dumpOutputFileName;
    bool dumpToFile = false;
    bool dumpToConsole = false;
    bool optimize = false;
    bool debug = true;
    bool proxyInlining = false;
    bool cuda = false;
    std::string cudaSdkPath;
    uint8_t optimizationLevel = 1;
};
}// namespace NES::Nautilus

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_UTIL_COMPILATIONOPTIONS_HPP_
