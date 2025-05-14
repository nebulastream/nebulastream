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
#include <InlineDataSourceRegistry.hpp>
#include <SystestState.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

InlineDataSourceRegistryReturnType
InlineDataSourceGeneratedRegistrar::RegisterFileInlineDataSource(InlineDataSourceRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.attachSource.tuples and not(systestAdaptorArguments.attachSource.tuples.value().empty()))
    {
        if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("filePath");
            filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            filePath->second = systestAdaptorArguments.testFilePath;
            if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
            {
                /// Write inline tuples to test file.
                for (const auto& tuple : systestAdaptorArguments.attachSource.tuples.value())
                {
                    testFile << tuple << "\n";
                }
                testFile.flush();
                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
        }
        throw InvalidConfigParameter("A FileSource config must contain filePath parameter");
    }
    throw TestException("A FileInlineSystestAdaptor requires inline tuples");
}

}
