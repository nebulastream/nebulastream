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

#pragma once

#include <functional>
#include <optional>
#include <string>

#include <Configurations/ConfigField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// A provider, so that the schema is built at first use and not during plugin loading.
using DecoderConfigSchemaProviderFn = std::function<Schema<QualifiedErasedConfigField, Ordered>()>;

class DecoderConfigSchemaRegistry
    : public RuntimeRegistry<DecoderConfigSchemaRegistry, std::string, DecoderConfigSchemaProviderFn, /*CaseSensitive*/ false>
{
public:
    static DecoderConfigSchemaRegistry& instance();

    [[nodiscard]] std::optional<Schema<QualifiedErasedConfigField, Ordered>> getSchema(const std::string& codec) const
    {
        if (const auto provider = find(codec))
        {
            return (*provider)();
        }
        return std::nullopt;
    }
};

}
