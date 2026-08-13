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

#pragma once

#include <ostream>
#include <string>
#include <string_view>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>
#include <Util/Variant.hpp>

namespace NES
{

/// Describes a configured input formatter instance: its type name plus the formatter-defined
/// config struct (e.g. CSVInputFormatterConfig), type-erased. The config is produced by the
/// formatter's InputFormatterConfigRegistry entry, so the formatter factory can safely any_cast
/// it back; serialization also goes through that entry. The NATIVE format carries no config,
/// represented by an empty std::any.
class InputFormatterDescriptor final
{
public:
    explicit InputFormatterDescriptor(Identifier inputFormatterType, ExplicitAny config);

    friend std::ostream& operator<<(std::ostream& out, const InputFormatterDescriptor& inputFormatterDescriptor);

    /// The type-erased config struct (std::any) is not comparable; two descriptors are considered
    /// equal if they describe the same formatter type.
    friend bool operator==(const InputFormatterDescriptor& lhs, const InputFormatterDescriptor& rhs)
    {
        return lhs.inputFormatterType == rhs.inputFormatterType;
    }

    [[nodiscard]] const Identifier& getInputFormatterType() const;

    [[nodiscard]] const ExplicitAny& getConfig() const;

    /// NOLINTNEXTLINE(cert-err58-cpp) - process aborts anyway if config schema setup throws at startup
    static inline auto TYPE_FIELD = ConfigField<Identifier>{
        Identifier::parse("TYPE"),
        "The type of the InputFormatter.",
        [](const ConfigLiteral& literal)
        { return tryGetOr<std::string>(literal, expectedType<std::string>()).and_then(Identifier::tryParse); }};

    /// NOLINTNEXTLINE(cert-err58-cpp) - process aborts anyway if config schema setup throws at startup
    static inline Schema<QualifiedErasedConfigField, Ordered> configSchema
        = createConfigSchema(Identifier::parse("INPUT_FORMATTER"), TYPE_FIELD);


private:
    friend struct Unreflector<InputFormatterDescriptor>;
    friend struct Reflector<InputFormatterDescriptor>;

    Identifier inputFormatterType;
    ExplicitAny config;
};

template <>
struct Reflector<InputFormatterDescriptor>
{
    Reflected operator()(const InputFormatterDescriptor& inputFormatterDescriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<InputFormatterDescriptor>
{
    InputFormatterDescriptor operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

}

FMT_OSTREAM(NES::InputFormatterDescriptor);
