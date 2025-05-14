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

#include <string>

#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <SystestAdaptor.hpp>
#include <SystestFileSourceRegistry.hpp>
#include <SystestState.hpp>


namespace
{
std::string replaceRootPath(const std::string& originalPath, const std::filesystem::path& newRootPath)
{
    if (const std::filesystem::path path(originalPath); path.has_parent_path()) //Todo: check that path is not absolute
    {
        if (const auto firstDir = path.begin(); *firstDir == std::filesystem::path("TESTDATA"))
        {
            return (newRootPath / path.lexically_relative(*firstDir)).string();
        }
        throw NES::InvalidConfigParameter(
        "The filepath of a FileSource config must contain begin with 'TESTDATA/', but got {}.", originalPath);
    }
    throw NES::InvalidConfigParameter(
        "The filepath of a FileSource config must contain at least a root directory and a file, but got {}.", originalPath);
}
}

namespace NES
{
SystestFileSourceRegistryReturnType
SystestFileSourceGeneratedRegistrar::RegisterFileSystestFileSource(SystestFileSourceRegistryArguments systestAdaptorArguments)
{
    /// Check that the test data dir is defined and that the 'filePath' parameter is set
    /// Replace the 'TESTDATA' placeholder in the filepath
    if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("filePath");
        filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
    {
        filePath->second = replaceRootPath(filePath->second, systestAdaptorArguments.testDataDir);
        return systestAdaptorArguments.physicalSourceConfig;
    }
    throw InvalidConfigParameter("A FileSource config must contain filePath parameter.");
}
}
