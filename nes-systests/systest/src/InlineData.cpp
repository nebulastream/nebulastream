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
#include <gtest/gtest-printers.h>

#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <InlineDataRegistry.hpp>
#include <SystestAdaptor.hpp>
#include <SystestState.hpp>
#include <TCPDataServer.hpp>

namespace NES
{

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
            auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.attachSource.tuples.value()));
            port->second = std::to_string(mockTCPServer->getPort());

            if (const auto host = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("socketHost");
                host != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                host->second = "localhost";
                auto serverThread
                    = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
                systestAdaptorArguments.attachSource.serverThreads->push_back(std::move(serverThread));

                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw InvalidConfigParameter("A TCP source config must contain a 'host' parameter");
        }
        throw InvalidConfigParameter("A TCP source config must contain a 'port' parameter");
    }
    throw TestException("A FileInlineSystestAdaptor requires inline tuples");
}

}
