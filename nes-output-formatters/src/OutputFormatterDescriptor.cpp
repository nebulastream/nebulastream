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

#include <OutputFormatters/OutputFormatterDescriptor.hpp>

#include <any>
#include <ostream>
#include <utility>
#include <variant>

#include <Identifiers/Identifier.hpp>
#include <Util/Any.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <OutputFormatterConfigRegistry.hpp>

namespace NES
{

namespace detail
{
struct ReflectedOutputFormatterDescriptor
{
    Identifier outputFormatterType;
    /// The formatter-defined config struct, reflected by the formatter's OutputFormatterConfigRegistry
    /// entry; empty for formats without a config (NATIVE).
    Reflected config;
};
}

const Identifier& OutputFormatterDescriptor::getOutputFormatterType() const
{
    return outputFormatterType;
}

bool OutputFormatterDescriptor::isNative() const
{
    return outputFormatterType == Identifier::parse("NATIVE");
}

const ExplicitAny& OutputFormatterDescriptor::getConfig() const
{
    return config;
}

OutputFormatterDescriptor::OutputFormatterDescriptor(Identifier outputFormatterType, ExplicitAny config)
    : outputFormatterType(std::move(outputFormatterType)), config(std::move(config))
{
}

OutputFormatterDescriptor OutputFormatterDescriptor::native()
{
    return OutputFormatterDescriptor{Identifier::parse("NATIVE"), ExplicitAny{std::any{std::monostate{}}}};
}

std::ostream& operator<<(std::ostream& out, const OutputFormatterDescriptor& outputFormatterDescriptor)
{
    return out << fmt::format("OutputFormatterDescriptor(type: {})", outputFormatterDescriptor.getOutputFormatterType());
}

Reflected Reflector<OutputFormatterDescriptor>::operator()(
    const OutputFormatterDescriptor& outputFormatterDescriptor, const ReflectionContext& context) const
{
    if (outputFormatterDescriptor.isNative())
    {
        return context.reflect(detail::ReflectedOutputFormatterDescriptor{
            .outputFormatterType = outputFormatterDescriptor.outputFormatterType, .config = Reflected{}});
    }

    const auto configEntry
        = OutputFormatterConfigRegistry::instance().find(outputFormatterDescriptor.outputFormatterType.asCanonicalString());
    INVARIANT(
        configEntry.has_value(),
        "Output formatter type {} has a descriptor but no OutputFormatterConfigRegistry entry",
        outputFormatterDescriptor.outputFormatterType);

    return context.reflect(detail::ReflectedOutputFormatterDescriptor{
        .outputFormatterType = outputFormatterDescriptor.outputFormatterType,
        .config = configEntry->reflect(outputFormatterDescriptor.config, context)});
}

OutputFormatterDescriptor Unreflector<OutputFormatterDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflectedOutputFormatterDescriptor = context.unreflect<detail::ReflectedOutputFormatterDescriptor>(rfl);

    if (reflectedOutputFormatterDescriptor.config.isEmpty())
    {
        return OutputFormatterDescriptor{
            std::move(reflectedOutputFormatterDescriptor.outputFormatterType), ExplicitAny{std::any{std::monostate{}}}};
    }

    const auto configEntry
        = OutputFormatterConfigRegistry::instance().find(reflectedOutputFormatterDescriptor.outputFormatterType.asCanonicalString());
    if (not configEntry.has_value())
    {
        throw UnknownOutputFormatterType(
            "Cannot deserialize output formatter descriptor: type {} has no OutputFormatterConfigRegistry entry",
            reflectedOutputFormatterDescriptor.outputFormatterType);
    }

    return OutputFormatterDescriptor{
        std::move(reflectedOutputFormatterDescriptor.outputFormatterType),
        ExplicitAny{configEntry->unreflect(reflectedOutputFormatterDescriptor.config, context)}};
}

}
