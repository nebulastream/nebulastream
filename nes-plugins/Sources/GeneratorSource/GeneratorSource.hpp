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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <ErrorHandling.hpp>
#include <FixedGeneratorRate.hpp>
#include <Generator.hpp>
#include <GeneratorFields.hpp>
#include <GeneratorRate.hpp>
#include <GeneratorSourceValidation.hpp>
#include <SinusGeneratorRate.hpp>

namespace NES
{

class GeneratorSource : public Source
{
public:
    constexpr static std::string_view NAME = "Generator";

    explicit GeneratorSource(const SourceDescriptor& sourceDescriptor);
    ~GeneratorSource() override = default;

    GeneratorSource(const GeneratorSource&) = delete;
    GeneratorSource& operator=(const GeneratorSource&) = delete;
    GeneratorSource(GeneratorSource&&) = delete;
    GeneratorSource& operator=(GeneratorSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

private:
    uint32_t seed;
    int32_t maxRuntime;
    uint64_t generatedTuplesCounter{0};
    uint64_t generatedBuffers{0};
    std::string generatorSchemaRaw;
    std::chrono::time_point<std::chrono::system_clock> generatorStartTime;
    Generator generator;
    std::stringstream tuplesStream;
    std::chrono::time_point<std::chrono::system_clock> startOfInterval;
    std::chrono::milliseconds flushInterval;
    std::unique_ptr<GeneratorRate> generatorRate;

    /// if inserting a set of generated tuples into the buffer would overflow it, this string saves them so it can be inserted into the next buffer
    std::string orphanTuples;
};

}
