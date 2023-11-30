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
#ifndef NES_COMMON_INCLUDE_UTIL_DUMPHELPER_HPP_
#define NES_COMMON_INCLUDE_UTIL_DUMPHELPER_HPP_
#include <memory>
#include <string>
namespace NES {

/**
 * @brief This is a utility, which provides an context, to dump state (e.g., query plans or irs) to a file or the console.
 */
class DumpHelper {
  public:
    /**
     * @brief Factory method to create the dump utility.
     * @param contextIdentifier the global identifier for all elements that are dumped by this context
     * @param dumpToConsole indicator if content is dumped to console
     * @param dumpToFile indicator if content is dumped to file
     * @param outputPath output path if content is dumped to a file
     * @return DumpHelper
     */
    static DumpHelper create(const std::string& contextIdentifier,
                             bool dumpToConsole,
                             bool dumpToFile,
                             const std::string& outputPath = std::string());

    /**
     * @brief Function to dump a specific entity using this context.
     * @param name identifier of this entry.
     * @param output the content that should be dumped.
     */
    void dump(const std::string_view& name, const std::string_view& output) const;

    /**
     * @brief get outputPath
     */
    const std::string& getOutputPath() const;

  private:
    explicit DumpHelper(std::string contextIdentifier, bool dumpToConsole, bool dumpToFile, std::string outputPath);
    const std::string contextIdentifier;
    const bool dumpToConsole;
    const bool dumpToFile;
    const std::string outputPath;
};
}// namespace NES

#endif  // NES_COMMON_INCLUDE_UTIL_DUMPHELPER_HPP_
