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

#include <TCPSinkDataCollector.hpp>

#include <array>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include <SinksParsing/SchemaFormatter.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/system/error_code.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

TCPSinkDataCollector::TCPSinkDataCollector(std::filesystem::path resultFilePath, Schema schema)
    : resultFilePath(std::move(resultFilePath)), schema(std::move(schema)), acceptor(ioContext, tcp::endpoint(tcp::v4(), 0))
{
}

TCPSinkDataCollector::~TCPSinkDataCollector()
{
    stop();
}

void TCPSinkDataCollector::run(const std::stop_token& stopToken)
{
    const std::stop_callback stopCallback(stopToken, [this]() { stop(); });

    tcp::socket socket(ioContext);
    boost::system::error_code error;
    acceptor.accept(socket, error);
    if (error || stopToken.stop_requested())
    {
        return;
    }

    std::string payload;
    std::array<char, 4096> buffer{};
    while (not stopToken.stop_requested())
    {
        const auto bytesRead = socket.read_some(boost::asio::buffer(buffer), error);
        if (bytesRead > 0)
        {
            payload.append(buffer.data(), bytesRead);
        }

        if (error == boost::asio::error::eof)
        {
            break;
        }
        if (error)
        {
            throw TestException("Failed to read TCP sink output: {}", error.message());
        }
    }

    writeResultFile(payload);
}

void TCPSinkDataCollector::stop()
{
    boost::system::error_code error;
    acceptor.cancel(error);
    acceptor.close(error);
    ioContext.stop();
}

void TCPSinkDataCollector::writeResultFile(const std::string& payload) const
{
    if (const auto parentPath = resultFilePath.parent_path(); not parentPath.empty())
    {
        std::filesystem::create_directories(parentPath);
    }

    std::ofstream resultFile(resultFilePath, std::ofstream::binary | std::ofstream::trunc);
    if (not resultFile.is_open())
    {
        throw TestException("Failed to open TCP sink result file: {}", resultFilePath.string());
    }

    auto schemaPtr = std::make_shared<const Schema>(schema);
    resultFile << SchemaFormatter(schemaPtr).getFormattedSchema();
    resultFile << payload;
    if (not payload.empty() and payload.back() != '\n')
    {
        resultFile << '\n';
    }
}

}
