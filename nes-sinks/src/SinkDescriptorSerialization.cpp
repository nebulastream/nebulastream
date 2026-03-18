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

#include <Sinks/SinkDescriptor.hpp>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/NESStrongTypeReflection.hpp> /// NOLINT(misc-include-cleaner)
#include <Util/Reflection.hpp>
#include <ProtobufHelper.hpp> /// NOLINT

namespace NES
{

Reflected Reflector<SinkDescriptor>::operator()(const SinkDescriptor& descriptor) const
{
    return reflect(detail::ReflectedSinkDescriptor{
        .sinkName = descriptor.sinkName,
        .schema = *descriptor.schema,
        .sinkType = descriptor.sinkType,
        .host = descriptor.host,
        .config = descriptor.getReflectedConfig(),
    });
}

SinkDescriptor Unreflector<SinkDescriptor>::operator()(const Reflected& reflected) const
{
    auto [name, schema, type, host, config] = unreflect<detail::ReflectedSinkDescriptor>(reflected);
    return SinkDescriptor{name, schema, type, host, Descriptor::unreflectConfig(config)};
}

}
