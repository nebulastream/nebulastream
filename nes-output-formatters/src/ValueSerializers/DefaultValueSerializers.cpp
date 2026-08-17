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

#include <ValueSerializers/DefaultValueSerializers.hpp>

#include <cstdint>
#include <memory>

#include <ValueSerializerRegistry.hpp>

namespace NES
{

namespace
{
/// The traced function is named after the C++ type it formats, not after the registry key, so that datatypes
/// sharing a body share its single instantiation -- INT8, INT16 and INT32 all serialize through int32_t.
template <typename T>
ValueSerializerRegistryReturnType makeFixedSize(const char* tracedName)
{
    return std::make_unique<DefaultFixedSizeValueSerializer<T>>(tracedName);
}
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultCHARValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<char>("DefaultChar");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultF32ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<float>("DefaultFloat");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultF64ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<double>("DefaultDouble");
}

/// INT8 and INT16 are formatted through int32_t: narrowing them to their own C++ types would make std::to_string
/// render them unsigned, so all three share the int32_t formatting function.
ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultINT8ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<int32_t>("DefaultInt32");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultINT16ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<int32_t>("DefaultInt32");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultINT32ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<int32_t>("DefaultInt32");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultINT64ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<int64_t>("DefaultInt64");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultBOOLValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<bool>("DefaultBool");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultUINT8ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<uint8_t>("DefaultUInt8");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultUINT16ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<uint16_t>("DefaultUInt16");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultUINT32ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<uint32_t>("DefaultUInt32");
}

ValueSerializerRegistryReturnType ValueSerializerGeneratedRegistrar::RegisterDefaultUINT64ValueSerializer(ValueSerializerRegistryArguments)
{
    return makeFixedSize<uint64_t>("DefaultUInt64");
}

ValueSerializerRegistryReturnType
ValueSerializerGeneratedRegistrar::RegisterDefaultVARSIZEDValueSerializer(ValueSerializerRegistryArguments args)
{
    return std::make_unique<DefaultVarSizedValueSerializer>(args.quoted);
}
}
