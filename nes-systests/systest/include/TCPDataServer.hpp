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

#include <functional>
#include <string>
#include <vector>
#include <boost/asio.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

class TCPDataServer
{
    using tcp = boost::asio::ip::tcp;
    using DataProvider = std::function<void(tcp::socket&)>;

public:
    // Constructor for vector of tuples
    explicit TCPDataServer(std::vector<std::string> tuples);
    // Constructor for filepath
    explicit TCPDataServer(std::filesystem::path filePath);

    TCPDataServer(const TCPDataServer&) = delete;
    TCPDataServer operator=(const TCPDataServer&) = delete;
    TCPDataServer(const TCPDataServer&&) = delete;
    TCPDataServer operator=(const TCPDataServer&&) = delete;

    ~TCPDataServer() { stop(); }

    uint16_t getPort() const { return acceptor.local_endpoint().port(); }

    void run(const std::stop_token& stopToken);

private:
    void startAccept(const std::stop_token& stopToken);

    void stop();

    boost::asio::io_context io_context;
    tcp::acceptor acceptor;
    DataProvider dataProvider; // Lambda function to provide data
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard;
};

}