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

#include <SemanticBackendFactory.hpp>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <HttpSemanticBackend.hpp>
#include <MockSemanticBackend.hpp>
#include <SemanticBackend.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

std::unique_ptr<SemanticBackend> SemanticBackendFactory::create(const SemanticModelConfig& config, std::optional<std::string> apiKey)
{
    if (config.backend == "http")
    {
        return std::make_unique<HttpSemanticBackend>(config.endpoint, std::move(apiKey));
    }
    if (config.backend == "mock")
    {
        NES_DEBUG(
            "Semantic model '{}' is answered by the mock backend ('{}'); nothing leaves the process", config.modelName, config.endpoint);
        std::vector<std::string> outputColumns;
        outputColumns.reserve(config.steps.size());
        for (const auto& step : config.steps)
        {
            outputColumns.push_back(step.outputColumn);
        }
        return std::make_unique<MockSemanticBackend>(config.endpoint, std::move(outputColumns));
    }
    throw InvalidSemanticModel("Unknown semantic model backend '{}' (expected http or mock)", config.backend);
}

}
