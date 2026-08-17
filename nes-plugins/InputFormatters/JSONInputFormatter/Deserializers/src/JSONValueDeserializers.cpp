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

#include <JSONValueDeserializers.hpp>

#include <memory>

#include <ValueDeserializerRegistry.hpp>

namespace NES
{

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterJSONCHARValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JsonValueDeserializer<false, true>>("JSONCHAR");
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterJSONVARSIZEDValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JsonValueDeserializer<false, false>>("JSONVARSIZED");
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableJSONCHARValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JsonValueDeserializer<true, true>>("NullableJSONCHAR");
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableJSONVARSIZEDValueDeserializer(ValueDeserializerRegistryArguments)
{
    return std::make_unique<JsonValueDeserializer<true, false>>("NullableJSONVARSIZED");
}
}
