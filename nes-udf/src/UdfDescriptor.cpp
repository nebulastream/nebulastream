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

#include <UdfDescriptor.hpp>

#include <filesystem>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

bool isSupportedUdfType(const DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::BOOLEAN:
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
        case DataType::Type::VARSIZED:
            return true;
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            return false;
    }
    return false;
}

namespace detail
{
struct ReflectedUdfDescriptor
{
    std::optional<std::string> name;
    std::optional<std::string> path;
    std::optional<std::string> entrypoint;
    std::optional<std::vector<DataType>> argTypes;
    std::optional<DataType> returnType;
};
}

Reflected Reflector<UdfDescriptor>::operator()(const UdfDescriptor& descriptor) const
{
    return reflect(detail::ReflectedUdfDescriptor{
        .name = std::make_optional(descriptor.getName()),
        .path = std::make_optional(descriptor.getPath().string()),
        .entrypoint = std::make_optional(descriptor.getEntrypoint()),
        .argTypes = std::make_optional(descriptor.getArgTypes()),
        .returnType = std::make_optional(descriptor.getReturnType())});
}

UdfDescriptor Unreflector<UdfDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<detail::ReflectedUdfDescriptor>(rfl);
    if (!reflected.name.has_value() || !reflected.path.has_value() || !reflected.entrypoint.has_value() || !reflected.argTypes.has_value()
        || !reflected.returnType.has_value())
    {
        throw NES::CannotDeserialize("Failed to deserialize UdfDescriptor");
    }
    /// Bypasses catalog validation: the coordinator already validated; the worker trusts the reflected form.
    return UdfDescriptor{
        std::move(reflected.name).value(),
        std::filesystem::path(std::move(reflected.path).value()),
        std::move(reflected.entrypoint).value(),
        std::move(reflected.argTypes).value(),
        std::move(reflected.returnType).value()};
}

}
