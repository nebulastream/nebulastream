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
#include <utility>

#include <Identifiers/Identifier.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// A decoder defined with CREATE DECODER <name> TYPE <codec> [SET (...)].
/// SourceDescriptors keep resolved copies, so that they are self-contained on a worker.
class DecoderDescriptor
{
public:
    [[nodiscard]] const Identifier& getName() const { return name; }

    [[nodiscard]] const Identifier& getType() const { return type; }

    [[nodiscard]] const ExplicitAny& getConfig() const { return config; }

    /// The config is type-erased and not comparable; a decoder is identified by its name and codec.
    friend bool operator==(const DecoderDescriptor& lhs, const DecoderDescriptor& rhs)
    {
        return lhs.name == rhs.name && lhs.type == rhs.type;
    }

    friend std::ostream& operator<<(std::ostream& out, const DecoderDescriptor& descriptor);

private:
    /// Only the catalog and deserialization create descriptors, and both reject unregistered codecs.
    friend class DecoderCatalog;
    friend struct Unreflector<DecoderDescriptor>;

    DecoderDescriptor(Identifier name, Identifier type, ExplicitAny config)
        : name(std::move(name)), type(std::move(type)), config(std::move(config))
    {
    }

    Identifier name;
    Identifier type;
    ExplicitAny config;
};

template <>
struct Reflector<DecoderDescriptor>
{
    Reflected operator()(const DecoderDescriptor& descriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<DecoderDescriptor>
{
    DecoderDescriptor operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

namespace NES::detail
{
struct ReflectedDecoderDescriptor
{
    Identifier name;
    Identifier type;
    Reflected config;
};
}

FMT_OSTREAM(NES::DecoderDescriptor);
