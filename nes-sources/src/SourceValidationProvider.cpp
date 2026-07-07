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

#include <Sources/SourceValidationProvider.hpp>

#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <Configurations/ConfigField.hpp>
#include <Schema/Schema.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SourceConfigSchemaRegistry.hpp>

namespace NES::SourceValidationProvider
{

std::optional<Schema<QualifiedErasedConfigField, Ordered>> provide(const std::string_view sourceType)
{
    auto sourceSchema = SourceConfigSchemaRegistry::instance().getSchema(std::string{sourceType});
    if (not sourceSchema.has_value())
    {
        return std::nullopt;
    }
    /// Combine the source-declared fields with the source-independent descriptor fields.
    auto combined = *sourceSchema | std::ranges::to<std::vector>();
    std::ranges::copy(SourceDescriptor::configSchema, std::back_inserter(combined));
    return Schema<QualifiedErasedConfigField, Ordered>{std::move(combined)};
}

}
