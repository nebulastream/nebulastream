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

#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <SystestAdaptor.hpp>
#include <SystestState.hpp>
#include <TCPDataServer.hpp>


namespace
{
std::filesystem::path replaceRootPath(const std::string& originalPath, const std::filesystem::path& newRootPath)
{
    if (const std::filesystem::path path(originalPath); not(path.is_absolute()))
    {
        if (const auto firstDir = path.begin(); *firstDir == std::filesystem::path("TESTDATA"))
        {
            return (newRootPath / path.lexically_relative(*firstDir)).string();
        }
        throw NES::InvalidConfigParameter(
            "The filepath of a FileSource config must contain begin with 'TESTDATA/', but got {}.", originalPath);
    }
    throw NES::InvalidConfigParameter(
        "The filepath of a FileSource config must contain at least a root directory and a file, but got {}.", originalPath);
}
}

namespace NES
{
FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    /// Check that the test data dir is defined and that the 'filePath' parameter is set
    /// Replace the 'TESTDATA' placeholder in the filepath
    if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("filePath");
        filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
    {
        filePath->second = replaceRootPath(filePath->second, systestAdaptorArguments.testDataDir);
        return systestAdaptorArguments.physicalSourceConfig;
    }
    throw InvalidConfigParameter("A FileData config must contain filePath parameter.");
}


FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterTCPFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("filePath");
        filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
    {
        if (const auto port = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(
                "socketPort"); //TOdo: implement in TCP source to access params?
            port != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            auto mockTCPServer = std::make_unique<TCPDataServer>(replaceRootPath(filePath->second, systestAdaptorArguments.testDataDir));
            port->second = std::to_string(mockTCPServer->getPort());

            if (const auto host = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find("socketHost");
                host != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                host->second = "localhost";
                auto serverThread
                    = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
                systestAdaptorArguments.attachSource.serverThreads->push_back(std::move(serverThread));

                systestAdaptorArguments.physicalSourceConfig.sourceConfig.erase("filePath");
                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw InvalidConfigParameter("A TCP source config must contain a 'host' parameter");
        }
        throw InvalidConfigParameter("A TCP source config must contain a 'port' parameter");
    }
    throw InvalidConfigParameter("A TCP source with FileData config must contain filePath parameter.");
}

}
