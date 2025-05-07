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

#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <utility>
#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

namespace NES
{

void DumpHelper::dump(std::string_view name, std::string_view output) const
{
    if (this->dumpToConsole)
    {
        NES_INFO("DUMP: {} {}", contextIdentifier, name);
        NES_INFO("{}", output);
    }
    if (not output.empty())
    {
        auto fileName = std::string{name};
        std::ranges::replace(fileName, ' ', '_');
        auto path = std::string{outputPath} + std::filesystem::path::preferred_separator + fileName;

        std::ofstream outputFile;
        outputFile.open(path);
        outputFile << output;
        outputFile.flush();
        outputFile.close();
        NES_INFO("DUMP {} {} to path file://{}", contextIdentifier, name, path);
    }
}

DumpHelper::DumpHelper(std::string contextIdentifier, const bool dumpToConsole)
    : contextIdentifier(std::move(contextIdentifier)), dumpToConsole(dumpToConsole)
{
}

DumpHelper::DumpHelper(std::string contextIdentifier, const bool dumpToConsole, std::string outputPath)
    : contextIdentifier(std::move(contextIdentifier)), dumpToConsole(dumpToConsole), outputPath(std::move(outputPath))
{
    try /// std::filesystem::current_path() can fail
    {
        std::string path = this->outputPath.empty() ? std::filesystem::current_path().string() : this->outputPath;
        path = path + std::filesystem::path::preferred_separator + "dump";
        if (!std::filesystem::is_directory(path))
        {
            std::filesystem::create_directory(path);
        }
        path = path + std::filesystem::path::preferred_separator
            + fmt::format("{}-{:%F %T}", this->contextIdentifier, std::chrono::system_clock::now());
        if (!std::filesystem::is_directory(path))
        {
            std::filesystem::create_directory(path);
        }
    }
    catch (const std::filesystem::filesystem_error&)
    {
        NES_WARNING("DumpHelper was configured to log to the file '{}', but could not create a valid path.", outputPath);
    }
}

}
