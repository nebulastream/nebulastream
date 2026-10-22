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

#include <Config/WorkerConfigResolution.hpp>

#include <iostream>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/Util.hpp>
#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <WorkerCatalogEntry.hpp>
#include <WorkerOptimizerConfig.hpp>

namespace NES
{
WorkerConfigResolver makeWorkerConfigResolver(
    Schema<LiteralConfigValue, Ordered> commandLineLiterals,
    Schema<LiteralConfigValue, Ordered> systestFileLiterals,
    const bool permitConflictingOptions,
    std::string overwriteContext)
{
    return [commandLineLiterals = std::move(commandLineLiterals),
            systestFileLiterals = std::move(systestFileLiterals),
            permitConflictingOptions,
            overwriteContext = std::move(overwriteContext)](const WorkerCatalogEntry& worker)
    {
        auto [literals, overwrites] = mergeConfigLayers(
            {ConfigLayer{.name = "command line", .literals = commandLineLiterals},
             ConfigLayer{.name = "topology", .literals = worker.config},
             ConfigLayer{.name = "systest file", .literals = systestFileLiterals}});
        if (!overwrites.empty())
        {
            const auto formatted = overwrites
                | std::views::transform(
                                       [](const ConfigOverwrite& overwrite)
                                       {
                                           return fmt::format(
                                               "{}: {} ({}) overwrites {} ({})",
                                               toLowerCase(fmt::format("{}", overwrite.name)),
                                               overwrite.appliedValue,
                                               overwrite.appliedLayer,
                                               overwrite.overwrittenValue,
                                               overwrite.overwrittenLayer);
                                       })
                | std::ranges::to<std::vector>();
            if (!permitConflictingOptions)
            {
                throw InvalidConfigParameter(
                    "Conflicting configuration values for worker {} ({}):\n{}\nPass --permit-conflicting-options to allow "
                    "higher-priority layers to overwrite.",
                    worker.host.getRawValue(),
                    overwriteContext,
                    fmt::join(formatted, "\n"));
            }
            for (const auto& line : formatted)
            {
                std::cout << fmt::format("[{}] worker {}: {}\n", overwriteContext, worker.host.getRawValue(), line);
            }
        }
        auto resolved = resolveConfiguration<WorkerOptimizerConfig>(literals);
        if (!resolved)
        {
            throw InvalidConfigParameter("{}", resolved.error());
        }
        NES_INFO(
            "Configuration of embedded worker {}:\n{}",
            worker.host.getRawValue(),
            formatEffectiveConfig(literals, WorkerOptimizerConfig::getConfigSchema()));
        return std::move(resolved)->worker;
    };
}

}
