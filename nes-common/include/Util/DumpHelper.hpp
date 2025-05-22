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
#pragma once

#include <string>

namespace NES
{

/// This is a utility, which provides an context, to dump state (e.g., query plans or irs) to a file or the console.
class DumpHelper
{
public:
    /// @param contextIdentifier the global identifier for all elements that are dumped by this context
    /// @param dumpToConsole dump logs to stdout
    explicit DumpHelper(std::string contextIdentifier, bool dumpToConsole);

    /// @param contextIdentifier the global identifier for all elements that are dumped by this context
    /// @param dumpToConsole dump logs to stdout
    /// @param outputPath if supplied, the DumpHelper dumps all logs to 'outputPath'
    explicit DumpHelper(std::string contextIdentifier, bool dumpToConsole, std::string outputPath);

    /**
     * @brief Function to dump a specific entity using this context.
     * @param name identifier of this entry.
     * @param output the content that should be dumped.
     */
    void dump(std::string_view name, std::string_view output) const;

private:
    std::string contextIdentifier;
    bool dumpToConsole;
    std::string outputPath;
};
}
