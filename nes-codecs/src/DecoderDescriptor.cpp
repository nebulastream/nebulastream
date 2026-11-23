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

#include <Codecs/DecoderDescriptor.hpp>

#include <ostream>
#include <utility>

#include <Identifiers/Identifier.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <DecoderConfigRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& out, const DecoderDescriptor& descriptor)
{
    return out << fmt::format("DecoderDescriptor(name: {}, type: {})", descriptor.getName(), descriptor.getType());
}

Reflected Reflector<DecoderDescriptor>::operator()(const DecoderDescriptor& descriptor, const ReflectionContext& context) const
{
    const auto entry = DecoderConfigRegistry::instance().find(descriptor.getType().asCanonicalString());
    PRECONDITION(entry.has_value(), "Unknown decoder type: {}", descriptor.getType());
    return context.reflect(
        detail::ReflectedDecoderDescriptor{
            .name = descriptor.getName(), .type = descriptor.getType(), .config = entry->reflect(descriptor.getConfig(), context)});
}

DecoderDescriptor Unreflector<DecoderDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [name, type, config] = context.unreflect<detail::ReflectedDecoderDescriptor>(reflected);
    const auto entry = DecoderConfigRegistry::instance().find(type.asCanonicalString());
    if (not entry.has_value())
    {
        throw CannotDeserialize("Unknown decoder type {}", type);
    }
    return DecoderDescriptor{std::move(name), std::move(type), entry->unreflect(config, context)};
}

}
