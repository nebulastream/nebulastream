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

#include <Sinks/PrintSink.hpp>

#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Variant.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{

/// Config fields of the print sink, shared by getConfigSchema (declaration) and
/// PrintSinkConfig::fromConfig (typed extraction).
/// NOLINTBEGIN(cert-err58-cpp)
const ConfigField<uint32_t> INGESTION{
    Identifier::parse("INGESTION"),
    "Artificial delay in milliseconds after emitting each buffer. 0 disables the delay.",
    [](const ConfigLiteral& literal)
    { return tryGetOr<int64_t>(literal, expectedType<uint32_t>()).and_then(downcastConfigValue<int64_t, uint32_t>); },
    uint32_t{0}};
/// NOLINTEND(cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> PrintSink::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("PRINT_SINK"), INGESTION);
}

std::expected<PrintSinkConfig, Exception> PrintSinkConfig::fromConfig(const InstantiatedConfig& config)
{
    return PrintSinkConfig{.ingestion = config.get(INGESTION)};
}

PrintSink::PrintSink(BackpressureController backpressureController, const PrintSinkConfig& config)
    : Sink(std::move(backpressureController)), outputStream(&std::cout), ingestion(config.ingestion)
{
}

void PrintSink::start(PipelineExecutionContext&)
{
}

void PrintSink::stop(PipelineExecutionContext&)
{
}

void PrintSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in PrintSink.");
    {
        const auto wlocked = outputStream.wlock();
        /// Create a buffer iterator to help iterate through the tuplebuffer and its children
        BufferIterator iterator{inputBuffer};

        std::optional<BufferIterator::BufferElement> element = iterator.getNextElement();
        while (element.has_value())
        {
            (*wlocked)->write(
                element.value().buffer.getAvailableMemoryArea<char>().data(), static_cast<std::streamsize>(element.value().contentLength));
            /// Get the next buffer to be written
            element = iterator.getNextElement();
        }
        (*wlocked)->flush();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ingestion});
}

std::ostream& PrintSink::toString(std::ostream& str) const
{
    str << fmt::format("PRINT_SINK");
    return str;
}

}
