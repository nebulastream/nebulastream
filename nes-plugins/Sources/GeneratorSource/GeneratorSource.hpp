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

#include <chrono>
#include <cstdint>
#include <memory>
#include <ostream>
#include <sstream>
#include <stop_token>
#include <string>
#include <string_view>
#include <variant>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Generator.hpp>
#include <GeneratorRate.hpp>

namespace NES
{

struct FixedGeneratorRateConfig
{
    double emitRate;
};

struct SinusGeneratorRateConfig
{
    double amplitude;
    double frequency;
};

using GeneratorRateVariant = std::variant<FixedGeneratorRateConfig, SinusGeneratorRateConfig>;

/// Source-defined config struct: instantiated from the generic config by the SourceConfig
/// registry entry, carried through the SourceDescriptor as std::any, and serialized via
/// reflection of exactly this struct (all members are reflectable).
struct GeneratorSourceConfig
{
    uint32_t seed;
    int32_t maxRuntime;
    std::string generatorSchemaRaw;
    GeneratorStop stopGeneratorWhenSequenceFinishes;
    std::chrono::milliseconds flushInterval;
    GeneratorRateVariant generatorRateConfig;

    static std::expected<GeneratorSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

class GeneratorSource : public Source
{
public:
    constexpr static std::string_view NAME = "Generator";

    explicit GeneratorSource(const GeneratorSourceConfig& config);
    ~GeneratorSource() override = default;

    GeneratorSource(const GeneratorSource&) = delete;
    GeneratorSource& operator=(const GeneratorSource&) = delete;
    GeneratorSource(GeneratorSource&&) = delete;
    GeneratorSource& operator=(GeneratorSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

private:
    GeneratorSourceConfig config;
    uint64_t generatedTuplesCounter{0};
    uint64_t generatedBuffers{0};
    std::chrono::time_point<std::chrono::system_clock> generatorStartTime;
    Generator generator;
    std::stringstream tuplesStream;
    std::chrono::time_point<std::chrono::system_clock> startOfInterval;
};

}
