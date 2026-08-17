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
#include <ValueDeserializers/DefaultValueDeserializers.hpp>

#include <cstdint>
#include <memory>

#include <ValueDeserializerRegistry.hpp>

namespace NES
{

namespace
{
/// The registry key doubles as the identity of the traced function, so that every field resolving to this plugin
/// shares one instantiation while two differently configured plugins stay apart.
template <typename T>
ValueDeserializerRegistryReturnType makeFixedSize(const char* typeKey, const bool quoted)
{
    return std::make_unique<DefaultFixedSizeValueDeserializer<T, false>>(typeKey, quoted);
}

template <typename T>
ValueDeserializerRegistryReturnType makeNullableFixedSize(const char* typeKey, const bool quoted)
{
    return std::make_unique<DefaultFixedSizeValueDeserializer<T, true>>(typeKey, quoted);
}
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultBOOLValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<bool>("DefaultBOOL", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultCHARValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return makeFixedSize<char>("DefaultCHAR", args.quoted);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultF32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<float>("DefaultF32", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultF64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<double>("DefaultF64", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultINT8ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<int8_t>("DefaultINT8", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultINT16ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<int16_t>("DefaultINT16", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultINT32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<int32_t>("DefaultINT32", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultINT64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<int64_t>("DefaultINT64", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultUINT8ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<uint8_t>("DefaultUINT8", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultUINT16ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<uint16_t>("DefaultUINT16", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultUINT32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<uint32_t>("DefaultUINT32", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultUINT64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeFixedSize<uint64_t>("DefaultUINT64", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterDefaultVARSIZEDValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultVarSizedValueDeserializer<false>>(args.quoted, args.hasTrailingSpaces);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultBOOLValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<bool>("DefaultBOOL", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultCHARValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return makeNullableFixedSize<char>("DefaultCHAR", args.quoted);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultF32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<float>("DefaultF32", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultF64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<double>("DefaultF64", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultINT8ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<int8_t>("DefaultINT8", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultINT16ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<int16_t>("DefaultINT16", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultINT32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<int32_t>("DefaultINT32", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultINT64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<int64_t>("DefaultINT64", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultUINT8ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<uint8_t>("DefaultUINT8", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultUINT16ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<uint16_t>("DefaultUINT16", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultUINT32ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<uint32_t>("DefaultUINT32", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultUINT64ValueDeserializer(ValueDeserializerRegistryArguments)
{
    return makeNullableFixedSize<uint64_t>("DefaultUINT64", false);
}

ValueDeserializerRegistryReturnType
ValueDeserializerGeneratedRegistrar::RegisterNullableDefaultVARSIZEDValueDeserializer(ValueDeserializerRegistryArguments args)
{
    return std::make_unique<DefaultVarSizedValueDeserializer<true>>(args.quoted, args.hasTrailingSpaces);
}
}
