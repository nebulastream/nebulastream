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
#include <boost/asio.hpp>
#include <boost/asio/buffer.hpp>
#include <gtest/gtest-printers.h>

#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <InlineDataRegistry.hpp>
#include <SystestAdaptor.hpp>
#include <SystestState.hpp>

namespace NES
{

class SyncedMockTcpServer
{
    using tcp = boost::asio::ip::tcp;

public:
    explicit SyncedMockTcpServer(std::vector<std::string> tuples)
        : acceptor(io_context, tcp::endpoint(tcp::v4(), 0)),
          tuples(std::move(tuples)),
          work_guard(boost::asio::make_work_guard(io_context))
    {
    }

    ~SyncedMockTcpServer()
    {
        stop();
    }

    uint16_t getPort() const { return acceptor.local_endpoint().port(); }

    void run(const std::stop_token& stopToken)
    {
        std::stop_callback stopCallback(stopToken, [this]() {
            stop();
        });

        startAccept(stopToken);
        io_context.run();
    }

private:
    void startAccept(const std::stop_token& stopToken)
    {
        if (stopToken.stop_requested()) {
            return;
        }

        auto socket = std::make_unique<tcp::socket>(io_context);

        acceptor.async_accept(*socket, [this, socket = std::move(socket), stopToken](const boost::system::error_code& error) {
            if (not error and not stopToken.stop_requested()) {
                sendTuplesAndCloseSocket(*socket);
            }
        });
    }

    void sendTuplesAndCloseSocket(tcp::socket& socket) const
    {
        for (const auto& tuple : tuples)
        {
            std::string data = tuple + "\n";
            boost::asio::write(socket, boost::asio::buffer(data));
        }

        boost::system::error_code errorCode;
        socket.shutdown(tcp::socket::shutdown_both, errorCode);
        socket.close(errorCode);
    }

    void stop()
    {
        boost::system::error_code ec;
        acceptor.cancel(ec);
        work_guard.reset();
        io_context.stop();
    }

    boost::asio::io_context io_context;
    tcp::acceptor acceptor;
    std::vector<std::string> tuples;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_guard;
};

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
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

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterTCPInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.attachSource.tuples and not(systestAdaptorArguments.attachSource.tuples.value().empty()))
    {
        if (const auto port = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(
                "socketPort"); //TOdo: implement in TCP source to access params?
            port != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            auto mockTCPServer = std::make_unique<SyncedMockTcpServer>(std::move(systestAdaptorArguments.attachSource.tuples.value()));
            port->second = std::to_string(mockTCPServer->getPort());

            if (const auto host = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("socketHost");
                host != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                host->second = "localhost";
                // systestAdaptorArguments.attachSource.serverThreads->resize(systestAdaptorArguments.attachSource.serverThreads->size() + 1);
                auto serverThread
                    = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
                systestAdaptorArguments.attachSource.serverThreads->push_back(std::move(serverThread));

                // auto serverThread = std::jthread([server = std::move(mockTCPServer)]() { NES_ERROR("hey ho"); });
                // systestAdaptorArguments.attachSource.serverThreads->emplace_back([mockTCPServer]() { mockTCPServer->run(); });
                // systestAdaptorArguments.attachSource.serverThreads->emplace_back([server = std::move(mockTCPServer)]() { NES_ERROR("hey ho"); });

                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw InvalidConfigParameter("A TCP source config must contain a 'host' parameter");
        }
        throw InvalidConfigParameter("A TCP source config must contain a 'port' parameter");
    }
    throw TestException("A FileInlineSystestAdaptor requires inline tuples");
}

}
