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

#include <Rules/Semantic/SemanticAsyncExecution.hpp>

#include <algorithm>
#include <cstddef>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <SemanticAsyncWiring.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

/// Enough room that the producer keeps the executor's threads busy without parking a large
/// share of the shared buffer pool: two buffers per concurrent call, never fewer than the
/// framework's own default.
constexpr size_t MinimumChannelCapacity = 64;

}

AsyncExecutionTrait semanticAsyncExecution(const RegisteredSemanticModel& model)
{
    const auto& config = model.getConfig();
    const auto canonicalNames = [](const SemanticFieldList& fields)
    {
        return fields
            | std::views::transform([](const UnqualifiedUnboundField& field)
                                    { return static_cast<const Identifier&>(field.getFullyQualifiedName()).asCanonicalString(); })
            | std::ranges::to<std::vector>();
    };

    auto encoded = encodeSemanticMapPayload(SemanticMapAsyncPayload{
        .config = config,
        .inputFields = canonicalNames(model.getSchema().inputs),
        .outputFields = canonicalNames(model.getSchema().outputs)});

    return AsyncExecutionTrait{
        model.hasFilterStep() ? "SemanticFilter" : "SemanticMap",
        {{std::string{SemanticMapConfigKey}, std::move(encoded)}},
        config.batchSize,
        config.maxConcurrency,
        std::max(MinimumChannelCapacity, 2 * config.maxConcurrency),
        config.preserveOrder};
}

}
