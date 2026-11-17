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

#include <Identifiers/Identifier.hpp>
#include <Util/Any.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// Describes one configured decoder stage: its name (an entry of SOURCE.DECODERS) plus the stage-defined
/// config struct, type-erased. The config is produced by the stage's DecoderStageConfigRegistry entry, so the
/// stage factory can safely any_cast it back; serialization also goes through that entry.
class DecoderStageDescriptor final
{
public:
    explicit DecoderStageDescriptor(Identifier stageType, ExplicitAny config);

    [[nodiscard]] const Identifier& getStageType() const;
    [[nodiscard]] const ExplicitAny& getConfig() const;

    /// The type-erased config is not comparable; descriptors of the same stage type compare equal.
    friend bool operator==(const DecoderStageDescriptor& lhs, const DecoderStageDescriptor& rhs) { return lhs.stageType == rhs.stageType; }

    friend std::ostream& operator<<(std::ostream& out, const DecoderStageDescriptor& descriptor);

private:
    friend struct Unreflector<DecoderStageDescriptor>;
    friend struct Reflector<DecoderStageDescriptor>;

    Identifier stageType;
    ExplicitAny config;
};

template <>
struct Reflector<DecoderStageDescriptor>
{
    Reflected operator()(const DecoderStageDescriptor& descriptor, const ReflectionContext& context) const;
};

template <>
struct Unreflector<DecoderStageDescriptor>
{
    DecoderStageDescriptor operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

}

FMT_OSTREAM(NES::DecoderStageDescriptor);
