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
#include <memory>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <variant>

#include <Model/RunnableTest.hpp>
#include <Rewriter/Constants.hpp>
#include <Util/Files.hpp>
#include <ErrorHandling.hpp>
#include <TCPDataServer.hpp>

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

RunningServer serve(const ServedData& data)
{
    /// The server binds in its constructor, so the port in the options is the one that the source connects to.
    auto server = std::visit([](const auto& content) { return std::make_unique<TCPDataServer>(content); }, data.content);
    const auto port = server->getPort();

    return {
        .thread = std::jthread{[owned = std::move(server)](const std::stop_token& stopToken) { owned->run(stopToken); }},
        .options
        = {Sql::option(Sql::Source, Sql::SocketHost, "localhost"),
           Sql::option(Sql::Source, Sql::SocketPort, std::to_string(port)),
           /// The test data sets are small, so a source would otherwise wait on a partial buffer for rows that never come.
           Sql::option(Sql::Source, Sql::FlushIntervalMs, "100")}};
}

}
