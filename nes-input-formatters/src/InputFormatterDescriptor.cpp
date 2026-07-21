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

#include <InputFormatterDescriptor.hpp>

#include <any>
#include <ostream>
#include <string>
#include <utility>

#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <InputFormatterConfigRegistry.hpp>
#include "Util/Variant.hpp"

namespace NES
{

namespace detail
{
struct ReflectedInputFormatterDescriptor
{
    Identifier inputFormatterType;
    /// The formatter-defined config struct, reflected by the formatter's InputFormatterConfigRegistry
    /// entry; empty for formats without a config (NATIVE).
    Reflected config;
};
}

const Identifier& InputFormatterDescriptor::getInputFormatterType() const
{
    return inputFormatterType;
}

const ExplicitAny& InputFormatterDescriptor::getConfig() const
{
    return config;
}

InputFormatterDescriptor::InputFormatterDescriptor(Identifier inputFormatterType, ExplicitAny config)
    : inputFormatterType(std::move(inputFormatterType)), config(std::move(config))
{
}

std::ostream& operator<<(std::ostream& out, const InputFormatterDescriptor& inputFormatterDescriptor)
{
    return out << fmt::format("InputFormatterDescriptor(type: {})", inputFormatterDescriptor.getInputFormatterType());
}

Reflected Reflector<InputFormatterDescriptor>::operator()(const InputFormatterDescriptor& inputFormatterDescriptor) const
{
    if (inputFormatterDescriptor.getInputFormatterType() == Identifier::parse("NATIVE")) {
        return reflect(detail::ReflectedInputFormatterDescriptor{.inputFormatterType = inputFormatterDescriptor.inputFormatterType, .config = Reflected{}});
    }

    const auto* configEntry = InputFormatterConfigRegistry::instance().find(inputFormatterDescriptor.inputFormatterType.asCanonicalString());
    INVARIANT(
        configEntry != nullptr,
        "Input formatter type {} has a descriptor but no InputFormatterConfigRegistry entry",
        inputFormatterDescriptor.inputFormatterType);

    return reflect(
        detail::ReflectedInputFormatterDescriptor{
            .inputFormatterType = inputFormatterDescriptor.inputFormatterType,
            .config = configEntry->reflect(inputFormatterDescriptor.config)});
}

InputFormatterDescriptor Unreflector<InputFormatterDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflectedInputFormatterDescriptor = context.unreflect<detail::ReflectedInputFormatterDescriptor>(rfl);

    if (reflectedInputFormatterDescriptor.config.isEmpty())
    {
        return InputFormatterDescriptor{std::move(reflectedInputFormatterDescriptor.inputFormatterType), ExplicitAny{std::any{}}};
    }

    const auto* configEntry = InputFormatterConfigRegistry::instance().find(reflectedInputFormatterDescriptor.inputFormatterType.asCanonicalString());
    if (configEntry == nullptr)
    {
        throw UnknownInputFormatterType(
            "Cannot deserialize input formatter descriptor: type {} has no InputFormatterConfigRegistry entry",
            reflectedInputFormatterDescriptor.inputFormatterType);
    }

    return InputFormatterDescriptor{
        std::move(reflectedInputFormatterDescriptor.inputFormatterType),
        ExplicitAny{configEntry->unreflect(reflectedInputFormatterDescriptor.config, context)}};
}

}
