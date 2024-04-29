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

#include <Util/DumpHelper.hpp>
#include <Util/Logger/Logger.hpp>
#include <filesystem>
#include <fstream>
#include <utility>

namespace NES {

DumpHelper
DumpHelper::create(const std::string& contextIdentifier, bool dumpToConsole, bool dumpToFile, const std::string& outputPath) {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << contextIdentifier << "-" << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d_%H:%M:%S");
    std::string path = outputPath.empty() ? std::filesystem::current_path().string() : outputPath;
    path = path + std::filesystem::path::preferred_separator + "dump";
    if (!std::filesystem::is_directory(path)) {
        std::filesystem::create_directory(path);
    }
    path = path + std::filesystem::path::preferred_separator + ss.str();
    if (!std::filesystem::is_directory(path)) {
        std::filesystem::create_directory(path);
    }
    return DumpHelper(ss.str(), dumpToConsole, dumpToFile, path);
}

void DumpHelper::dump(const std::string_view& name, const std::string_view& output) const {
    if (this->dumpToConsole) {
        NES_INFO("DUMP: {} {}", contextIdentifier, name);
        NES_INFO("{}", output);
    }
    if (this->dumpToFile) {
        auto fileName = std::string{name};
        std::replace(fileName.begin(), fileName.end(), ' ', '_');
        auto path = std::string{outputPath} + std::filesystem::path::preferred_separator + fileName;

        std::ofstream outputFile;
        outputFile.open(path);
        outputFile << output;
        outputFile.flush();
        outputFile.close();
        NES_INFO("DUMP {} {} to path file://{}", contextIdentifier, name, path);
    }
}

DumpHelper::DumpHelper(std::string contextIdentifier, bool dumpToConsole, bool dumpToFile, std::string outputPath)
    : contextIdentifier(std::move(contextIdentifier)), dumpToConsole(dumpToConsole), dumpToFile(dumpToFile),
      outputPath(std::move(outputPath)) {}

}// namespace NES
