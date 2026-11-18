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

#include <Runner/DataStaging.hpp>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <Model/RunnableTestFile.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void writeInlineData(const InlineData& data)
{
    std::filesystem::create_directories(data.path.parent_path());
    std::ofstream file{data.path};
    for (const auto& row : data.rows)
    {
        file << row << '\n';
    }
    /// A source that cannot open this file reports only that the path does not resolve.
    /// Failing here reports the write that did not happen.
    file.flush();
    if (not file)
    {
        throw TestException("could not write the inline data of a source to {}: {}", data.path.string(), getErrorMessageFromERRNO());
    }
}

std::vector<std::string> stage(const RunnableTestFile& runnable)
{
    std::vector<std::string> sql;
    for (const auto& setup : runnable.setupStatements)
    {
        if (setup.inlineData.has_value())
        {
            writeInlineData(*setup.inlineData);
        }
        sql.push_back(setup.sql);
    }
    return sql;
}

}
