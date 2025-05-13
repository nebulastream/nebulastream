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
#include <gtest/gtest-printers.h>


#include <SystestAdaptor.hpp>
#include <SystestAdaptorRegistry.hpp>
#include <SystestState.hpp>


namespace
{
std::string replaceRootPath(const std::string& originalPath, const std::filesystem::path& newRootPath)
{
    const std::filesystem::path path(originalPath);
    const auto relativePath = path.has_root_path() ? path.relative_path() : path;
    return (newRootPath / relativePath).string();
}
}

namespace NES
{
SystestAdaptorRegistryReturnType
SystestAdaptorGeneratedRegistrar::RegisterFileSourceInlineSystestAdaptor(SystestAdaptorRegistryArguments systestAdaptorArguments)
{
    if (not systestAdaptorArguments.attachSource.tuples or systestAdaptorArguments.attachSource.tuples.value().empty())
    {
        throw CannotLoadConfig("A FileInlineSystestAdaptor requires inline tuples");
    }
    if (const auto sourceFile = systestAdaptorArguments.testFilePath)
    {
        if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("filePath");
            filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            filePath->second = sourceFile.value();
        }
        else
        {
            throw InvalidConfigParameter("A FileSource config must contain filePath parameter");
        }
        std::ofstream testFile(sourceFile.value());
        if (!testFile.is_open())
        {
            throw TestException("Could not open source file \"{}\"", sourceFile.value());
        }

        /// Write inline tuples to test file.
        for (const auto& tuple : systestAdaptorArguments.attachSource.tuples.value())
        {
            testFile << tuple << "\n";
        }
        testFile.flush();
        NES_INFO(
            "Written in file: {}. Number of Tuples: {}", sourceFile.value(), systestAdaptorArguments.attachSource.tuples.value().size());

        /// Nothing to initiate
        return {};
    }
    throw CannotLoadConfig("A FileInlineSystestAdaptor requires a file path, but got nullopt");
}

SystestAdaptorRegistryReturnType
SystestAdaptorGeneratedRegistrar::RegisterFileSourceFileSystestAdaptor(SystestAdaptorRegistryArguments systestAdaptorArguments)
{
    /// Check that the test data dir is defined and that the 'filePath' parameter is set
    /// Replace the 'TESTDATA' placeholder in the filepath
    if (not systestAdaptorArguments.testDataDir)
    {
        throw CannotLoadConfig("A FileSourceFileSystestAdaptor requires a test data dir.");
    }
    if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("filePath");
        filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
    {
        filePath->second = replaceRootPath(filePath->second, systestAdaptorArguments.testDataDir.value());
    }
    else
    {
        throw InvalidConfigParameter("A FileSource config must contain filePath parameter.");
    }

    return {};
}
/// Not supporting 'Generator' adaptor type
}
