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

#include <Sinks/SinkDataProvider.hpp>

#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ErrorHandling.hpp>
#include <SinkCaptureRegistry.hpp>

namespace NES
{

bool SinkDataProvider::hasCaptureFor(const std::string& sinkType)
{
    return SinkCaptureRegistry::instance().contains(sinkType);
}

std::unordered_map<std::string, std::string> SinkDataProvider::provideSinkCapture(
    const std::string& sinkType,
    std::unordered_map<std::string, std::string> sinkConfig,
    std::filesystem::path resultFile,
    std::shared_ptr<std::vector<std::jthread>> serverThreads,
    std::shared_ptr<std::function<void()>> resultFinalizer)
{
    auto captureArgs = SinkCaptureRegistryArguments{
        .sinkConfig = std::move(sinkConfig),
        .resultFile = std::move(resultFile),
        .serverThreads = std::move(serverThreads),
        .resultFinalizer = std::move(resultFinalizer)};
    if (auto rewrittenConfig = SinkCaptureRegistry::instance().create(sinkType, captureArgs))
    {
        return rewrittenConfig.value();
    }
    throw InvalidConfigParameter("No sink capture registrar for sink type {}.", sinkType);
}

}
