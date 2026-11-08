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

#include <PythonProcessSource.hpp>

#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <utility>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <PythonBatchWrapper.hpp>
#include <PythonProcesses.hpp>
#include <PythonProcessShmSource.hpp>
#include <PythonProcessSocketSource.hpp>

namespace NES
{

namespace
{
/// NOLINTBEGIN(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::string> CHANNEL{
    Identifier::parse("CHANNEL"),
    "Id of the channel the PythonProcess sink of the same UDF operator hands its buffers to",
    [](const ConfigLiteral& literal) { return NES::tryGetOr<std::string>(literal, expectedType<std::string>()); }};

const ConfigField<std::string> CALLS{
    Identifier::parse("CALLS"),
    "The serialized calls of the UDF operator",
    [](const ConfigLiteral& literal) { return NES::tryGetOr<std::string>(literal, expectedType<std::string>()); }};
/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

std::unique_ptr<Source> makeImplementation(const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor)
{
    const auto protocol = PythonProcesses::protocol();
    if (protocol == UdfProcessProtocol::SHARED_MEMORY || protocol == UdfProcessProtocol::SHARED_MEMORY_ZERO_COPY)
    {
        return std::make_unique<PythonProcessShmSource>(config, sourceDescriptor, protocol);
    }
    return std::make_unique<PythonProcessSocketSource>(config, sourceDescriptor, protocol);
}
}

Schema<QualifiedErasedConfigField, Ordered> PythonProcessSource::getConfigSchema()
{
    return createConfigSchema(Identifier::parse("PYTHON_PROCESS_SOURCE"), CHANNEL, CALLS);
}

std::expected<PythonProcessSourceConfig, Exception> PythonProcessSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    return PythonProcessSourceConfig{.channel = config.get(CHANNEL), .calls = config.get(CALLS)};
}

PythonProcessSource::PythonProcessSource(const PythonProcessSourceConfig& config, const SourceDescriptor& sourceDescriptor)
    : implementation(makeImplementation(config, sourceDescriptor))
{
}

Source::FillTupleBufferResult PythonProcessSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    return implementation->fillTupleBuffer(tupleBuffer, stopToken);
}

bool PythonProcessSource::providesBuffers() const
{
    return implementation->providesBuffers();
}

std::optional<TupleBuffer> PythonProcessSource::nextBuffer(const std::stop_token& stopToken)
{
    return implementation->nextBuffer(stopToken);
}

void PythonProcessSource::open(std::shared_ptr<AbstractBufferProvider> bufferProvider)
{
    implementation->open(std::move(bufferProvider));
}

void PythonProcessSource::close()
{
    implementation->close();
}

std::ostream& PythonProcessSource::toString(std::ostream& str) const
{
    return str << *implementation;
}

}
