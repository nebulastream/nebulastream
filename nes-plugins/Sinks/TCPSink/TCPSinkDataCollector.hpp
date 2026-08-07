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

#include <cstdint>
#include <filesystem>
#include <stop_token>
#include <string>

#include <DataTypes/Schema.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace NES
{

class TCPSinkDataCollector
{
    using tcp = boost::asio::ip::tcp;

public:
    TCPSinkDataCollector(std::filesystem::path resultFilePath, Schema schema);
    ~TCPSinkDataCollector();

    TCPSinkDataCollector(const TCPSinkDataCollector&) = delete;
    TCPSinkDataCollector& operator=(const TCPSinkDataCollector&) = delete;
    TCPSinkDataCollector(TCPSinkDataCollector&&) = delete;
    TCPSinkDataCollector& operator=(TCPSinkDataCollector&&) = delete;

    [[nodiscard]] uint16_t getPort() const { return acceptor.local_endpoint().port(); }

    void run(const std::stop_token& stopToken);

private:
    void stop();
    void writeResultFile(const std::string& payload) const;

    std::filesystem::path resultFilePath;
    Schema schema;
    boost::asio::io_context ioContext;
    tcp::acceptor acceptor;
};

}
