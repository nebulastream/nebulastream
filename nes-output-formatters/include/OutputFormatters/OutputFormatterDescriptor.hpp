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

#include <Configurations/ConfigField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>
#include <Util/Variant.hpp>

namespace NES
{

/// Describes a configured output formatter instance: its type name plus the formatter-defined
/// config struct (e.g. CSVOutputFormatterConfig), type-erased. The config is produced by the
/// formatter's OutputFormatterConfigRegistry entry, so the formatter factory can safely any_cast
/// it back; serialization also goes through that entry. The NATIVE format carries no config,
/// represented by a std::monostate.
class OutputFormatterDescriptor final
{
public:
    explicit OutputFormatterDescriptor(Identifier outputFormatterType, ExplicitAny config);

    /// The NATIVE format forwards buffers without formatting; it has no registry entry and no config.
    static OutputFormatterDescriptor native();

    friend std::ostream& operator<<(std::ostream& out, const OutputFormatterDescriptor& outputFormatterDescriptor);

    /// The type-erased config struct (std::any) is not comparable; two descriptors are considered
    /// equal if they describe the same formatter type.
    friend bool operator==(const OutputFormatterDescriptor& lhs, const OutputFormatterDescriptor& rhs)
    {
        return lhs.outputFormatterType == rhs.outputFormatterType;
    }

    [[nodiscard]] const Identifier& getOutputFormatterType() const;
    [[nodiscard]] bool isNative() const;

    [[nodiscard]] const ExplicitAny& getConfig() const;

    /// Defaults to NATIVE: a sink without an output formatter forwards buffers unformatted.
    static inline auto TYPE_FIELD = ConfigField<Identifier>{
        Identifier::parse("TYPE"),
        "The type of the output form",
        [](const ConfigLiteral& literal)
        { return tryGetOr<std::string>(literal, expectedType<std::string>()).and_then(Identifier::tryParse); },
        Identifier::parse("NATIVE")};

    static inline Schema<QualifiedErasedConfigField, Ordered> configSchema
        = createConfigSchema(Identifier::parse("OUTPUT_FORMATTER"), TYPE_FIELD);

private:
    friend struct Unreflector<OutputFormatterDescriptor>;
    friend struct Reflector<OutputFormatterDescriptor>;

    Identifier outputFormatterType;
    ExplicitAny config;
};

template <>
struct Reflector<OutputFormatterDescriptor>
{
    Reflected operator()(const OutputFormatterDescriptor& outputFormatterDescriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<OutputFormatterDescriptor>
{
    OutputFormatterDescriptor operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

}

FMT_OSTREAM(NES::OutputFormatterDescriptor);
