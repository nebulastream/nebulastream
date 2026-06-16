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

/// Systest data adaptors for the File source. The File source runtime + validation
/// now live in the Rust file_source plugin; these adaptors only prepare test data
/// (write inline tuples to a temp file / point at an existing file) and set the
/// FILE_PATH config the source reads. They register into the InlineData/FileData
/// registries consumed by the systest harness.

#include <fstream>
#include <string>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifier.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <FileSourceConfig.hpp>
#include <InlineDataRegistry.hpp>

namespace NES
{

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    const auto filePathParameter = Identifier::parse(std::string(SYSTEST_FILE_PATH_PARAMETER));
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(filePathParameter))
    {
        throw InvalidConfigParameter("Mock FileSource cannot use given inline data if a 'file_path' is set");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.try_emplace(filePathParameter, systestAdaptorArguments.testFilePath.string());


    if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
    {
        /// Write inline tuples to test file.
        for (const auto& tuple : systestAdaptorArguments.tuples)
        {
            testFile << tuple << "\n";
        }
        testFile.flush();
        return systestAdaptorArguments.physicalSourceConfig;
    }
    throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    const auto filePathParameter = Identifier::parse(std::string(SYSTEST_FILE_PATH_PARAMETER));
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(filePathParameter))
    {
        throw InvalidConfigParameter("The mock file data source cannot be used if the file_path parameter is already set.");
    }

    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(filePathParameter, systestAdaptorArguments.testFilePath.string());

    return systestAdaptorArguments.physicalSourceConfig;
}

}
