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

#include <Decoders/DecoderStageDescriptor.hpp>

#include <ostream>
#include <utility>
#include <Identifiers/Identifier.hpp>
#include <Util/Any.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <DecoderStageConfigRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
struct ReflectedDecoderStageDescriptor
{
    Identifier stageType;
    /// The stage-defined config struct, reflected by the stage's DecoderStageConfigRegistry entry.
    Reflected config;
};
}

DecoderStageDescriptor::DecoderStageDescriptor(Identifier stageType, ExplicitAny config)
    : stageType(std::move(stageType)), config(std::move(config))
{
}

const Identifier& DecoderStageDescriptor::getStageType() const
{
    return stageType;
}

const ExplicitAny& DecoderStageDescriptor::getConfig() const
{
    return config;
}

std::ostream& operator<<(std::ostream& out, const DecoderStageDescriptor& descriptor)
{
    return out << fmt::format("DecoderStageDescriptor(type: {})", descriptor.stageType);
}

Reflected Reflector<DecoderStageDescriptor>::operator()(const DecoderStageDescriptor& descriptor, const ReflectionContext& context) const
{
    const auto configEntry = DecoderStageConfigRegistry::instance().find(descriptor.stageType.asCanonicalString());
    INVARIANT(configEntry.has_value(), "Decoder stage {} has a descriptor but no DecoderStageConfigRegistry entry", descriptor.stageType);
    return context.reflect(
        ReflectedDecoderStageDescriptor{.stageType = descriptor.stageType, .config = configEntry->reflect(descriptor.config, context)});
}

DecoderStageDescriptor Unreflector<DecoderStageDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflected = context.unreflect<ReflectedDecoderStageDescriptor>(rfl);
    const auto configEntry = DecoderStageConfigRegistry::instance().find(reflected.stageType.asCanonicalString());
    if (not configEntry.has_value())
    {
        throw UnknownDecoderType(
            "Cannot unreflect decoder stage descriptor: {} has no DecoderStageConfigRegistry entry", reflected.stageType);
    }
    return DecoderStageDescriptor{std::move(reflected.stageType), ExplicitAny{configEntry->unreflect(reflected.config, context)}};
}

}
