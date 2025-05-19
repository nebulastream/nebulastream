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

#include <functional>
#include <string>
#include <utility>
#include <vector>
#include <boost/asio.hpp>
#include <boost/asio/buffer.hpp>

#include <DataServer/TCPDataServer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
TCPDataServer::TCPDataServer(std::vector<std::string> tuples)
    : acceptor(io_context, tcp::endpoint(tcp::v4(), 0)), work_guard(boost::asio::make_work_guard(io_context))
{
    dataProvider = [tuples = std::move(tuples)](tcp::socket& socket)
    {
        for (const auto& tuple : tuples)
        {
            std::string data = tuple + "\n";
            boost::asio::write(socket, boost::asio::buffer(data));
        }
    };
}

TCPDataServer::TCPDataServer(std::filesystem::path filePath)
    : acceptor(io_context, tcp::endpoint(tcp::v4(), 0)), work_guard(boost::asio::make_work_guard(io_context))
{
    if (not std::filesystem::exists(filePath))
    {
        throw TestException("File to serve TCP data from does not exist: {}", filePath.string());
    }

    dataProvider = [filePath = std::move(filePath)](tcp::socket& socket)
    {
        std::ifstream file(filePath);
        if (not file.is_open())
        {
            throw TestException("Failed to open file to serve TCP data from: {}", filePath.string());
        }

        std::string line;
        while (std::getline(file, line))
        {
            std::string data = line + "\n";
            boost::asio::write(socket, boost::asio::buffer(data));
        }
    };
}

void TCPDataServer::run(const std::stop_token& stopToken)
{
    std::stop_callback stopCallback(stopToken, [this]() { stop(); });

    startAccept(stopToken);
    io_context.run();
}

void TCPDataServer::startAccept(const std::stop_token& stopToken)
{
    if (stopToken.stop_requested())
    {
        return;
    }

    auto socket = std::make_shared<tcp::socket>(io_context);

    acceptor.async_accept(
        *socket,
        [this, socket, stopToken](const boost::system::error_code& error)
        {
            if (not error and not stopToken.stop_requested())
            {
                handleConnection(socket, stopToken);
                startAccept(stopToken);
            }
        });
}

void TCPDataServer::handleConnection(std::shared_ptr<tcp::socket> socket, const std::stop_token& stopToken)
{
    boost::asio::post(
        io_context,
        [this, socket, stopToken]()
        {
            if (stopToken.stop_requested())
            {
                return;
            }

            try
            {
                dataProvider(*socket);

                socket->shutdown(tcp::socket::shutdown_send);

                boost::system::error_code ec;
                std::array<char, 1> dummy;
                while (socket->read_some(boost::asio::buffer(dummy), ec) > 0)
                {
                }

                socket->close();
            }
            catch (const std::exception&)
            {
                boost::system::error_code ec;
                socket->close(ec);
            }
        });
}

void TCPDataServer::stop()
{
    boost::system::error_code ec;
    if (const auto errorCode = acceptor.cancel(ec); errorCode.failed())
    {
        throw TestException("Failed to cancel acceptor: {}", errorCode.message());
    }
    work_guard.reset();
    io_context.stop();
}

}
