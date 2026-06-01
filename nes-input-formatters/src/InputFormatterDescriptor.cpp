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

#include <ostream>
#include <string>
#include <utility>
#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

const std::string& InputFormatterDescriptor::getInputFormatterType() const
{
    return inputFormatterType;
}

InputFormatterDescriptor::InputFormatterDescriptor(std::string inputFormatterType, DescriptorConfig::Config config)
    : Descriptor(std::move(config)), inputFormatterType(std::move(inputFormatterType))
{
}

std::ostream& operator<<(std::ostream& out, const InputFormatterDescriptor& inputFormatterDescriptor)
{
    return out << fmt::format("InputFormatterDescriptor: (Config: {})", inputFormatterDescriptor.toStringConfig());
}

Reflected Reflector<InputFormatterDescriptor>::operator()(
    const InputFormatterDescriptor& inputFormatterDescriptor, const ReflectionContext& context) const
{
    const detail::ReflectedInputFormatterDescriptor descriptor{
        .inputFormatterType = inputFormatterDescriptor.inputFormatterType, .config = inputFormatterDescriptor.getReflectedConfig(context)};

    return context.reflect(descriptor);
}

InputFormatterDescriptor Unreflector<InputFormatterDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflectedInputFormatterDescriptor = context.unreflect<detail::ReflectedInputFormatterDescriptor>(rfl);

    return InputFormatterDescriptor{
        reflectedInputFormatterDescriptor.inputFormatterType,
        Descriptor::unreflectConfig(reflectedInputFormatterDescriptor.config, context)};
}


}
