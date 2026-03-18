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

#include <Sources/SourceDescriptor.hpp>

#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeReflection.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/LogicalSource.hpp>
#include <Util/Reflection.hpp>
#include <ProtobufHelper.hpp> /// NOLINT

namespace NES
{

Reflected Reflector<SourceDescriptor>::operator()(const SourceDescriptor& sourceDescriptor) const
{
    const detail::ReflectedSourceDescriptor descriptor{
        .physicalSourceId = sourceDescriptor.physicalSourceId.getRawValue(),
        .logicalSource = sourceDescriptor.logicalSource,
        .type = sourceDescriptor.sourceType,
        .host = sourceDescriptor.host,
        .parserConfig = sourceDescriptor.parserConfig,
        .config = sourceDescriptor.getReflectedConfig()};

    return reflect(descriptor);
}

SourceDescriptor Unreflector<SourceDescriptor>::operator()(const Reflected& rfl) const
{
    auto reflectedSourceDescriptor = unreflect<detail::ReflectedSourceDescriptor>(rfl);

    return SourceDescriptor{
        PhysicalSourceId{reflectedSourceDescriptor.physicalSourceId},
        LogicalSource{std::move(reflectedSourceDescriptor.logicalSource)},
        reflectedSourceDescriptor.type,
        reflectedSourceDescriptor.host,
        Descriptor::unreflectConfig(reflectedSourceDescriptor.config),
        std::move(reflectedSourceDescriptor.parserConfig)};
}
}
