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

#include <TCPSink.hpp>
#include <TCPSinkDataCollector.hpp>

#include <memory>
#include <string>
#include <thread>
#include <utility>

#include <SinkDataRegistry.hpp>

#include <ErrorHandling.hpp>

namespace NES
{

SinkDataRegistryReturnType SinkDataGeneratedRegistrar::RegisterTCPSinkData(SinkDataRegistryArguments sinkDataArguments)
{
    if (sinkDataArguments.physicalSinkConfig.sinkConfig.contains(ConfigParametersTCPSink::PORT))
    {
        throw InvalidConfigParameter("Cannot use TCP sink test collector if config already contains a socket_port");
    }
    if (sinkDataArguments.physicalSinkConfig.sinkConfig.contains(ConfigParametersTCPSink::HOST))
    {
        throw InvalidConfigParameter("Cannot use TCP sink test collector if config already contains a socket_host");
    }

    auto collector = std::make_unique<TCPSinkDataCollector>(
        sinkDataArguments.physicalSinkConfig.resultFilePath, sinkDataArguments.physicalSinkConfig.schema);

    sinkDataArguments.physicalSinkConfig.sinkConfig.emplace(ConfigParametersTCPSink::PORT, std::to_string(collector->getPort()));
    sinkDataArguments.physicalSinkConfig.sinkConfig.emplace(ConfigParametersTCPSink::HOST, "localhost");

    sinkDataArguments.serverThreads->emplace_back(
        [collector = std::move(collector)](const std::stop_token& stopToken) { collector->run(stopToken); });

    return sinkDataArguments.physicalSinkConfig;
}

}
