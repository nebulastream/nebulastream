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

#include <ValueDeserializerUtil.hpp>

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include <DataTypes/DataType.hpp>
#include <ErrorHandling.hpp>
#include <ValueDeserializer.hpp>
#include <ValueDeserializerRegistry.hpp>

namespace NES
{
std::unique_ptr<ValueDeserializer> provideValueDeserializer(const std::string& deserializerType, const ValueDeserializerConfig& config)
{
    /// Resolve the "Nullable" member
    const std::string completeDeserializerName = config.nullable ? "Nullable" + deserializerType : deserializerType;
    const ValueDeserializerRegistryArguments arguments{.quoted = config.quoted, .hasTrailingSpaces = config.hasTrailingSpaces};
    if (const auto deserializerFactory = ValueDeserializerRegistry::instance().find(completeDeserializerName))
    {
        return (*deserializerFactory)(arguments);
    }
    throw UnknownValueDeserializerType("Unknown Value Deserializer: {}", deserializerType);
}
}
