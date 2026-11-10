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

#include <Sources/SourceProvider.hpp>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <BackpressureChannel.hpp>
#include <CpuPinning.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>

namespace NES
{

SourceProvider::SourceProvider(size_t defaultMaxInflightBuffers, std::shared_ptr<AbstractBufferProvider> bufferPool)
    : defaultMaxInflightBuffers(defaultMaxInflightBuffers), bufferPool(std::move(bufferPool))
{
}

std::unique_ptr<SourceHandle>
SourceProvider::lower(OriginId originId, BackpressureListener backpressureListener, const SourceDescriptor& sourceDescriptor) const
{
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    if (const auto sourceFactory = SourceRegistry::instance().find(sourceDescriptor.getSourceType().asCanonicalString()))
    {
        auto source = (*sourceFactory)(sourceDescriptor);
        /// The source-specific configuration of maxInflightBuffers takes priority.
        /// If not specified, we take the NodeEngine-wide configuration.
        const auto maxInflightBuffers = sourceDescriptor.getMaxInflightBuffers().value_or(defaultMaxInflightBuffers);
        /// A source that reads bytes, such as a file, in the NATIVE format: the bytes are the tuples, and nothing formats them. Sources
        /// that add metadata pass on tuple buffers and count the tuples themselves.
        size_t rawTupleSize = 0;
        if (sourceDescriptor.getInputFormatType() == Identifier::parse("NATIVE") && !source->addsMetadata())
        {
            const auto& schema = sourceDescriptor.getSchema();
            if (std::ranges::any_of(schema, [](const auto& field) { return field.getDataType().type == DataType::Type::VARSIZED; }))
            {
                throw InvalidConfigParameter(
                    "A {} source cannot read VARSIZED fields in the NATIVE format", sourceDescriptor.getSourceType());
            }
            rawTupleSize = getSizeInBytes(schema);
            if (rawTupleSize == 0 || bufferPool->getBufferSize() % rawTupleSize != 0)
            {
                throw InvalidConfigParameter(
                    "The NATIVE format needs buffers that hold whole tuples, but a tuple has {} bytes and a buffer {}",
                    rawTupleSize,
                    bufferPool->getBufferSize());
            }
        }
        const SourceRuntimeConfiguration runtimeConfig{
            .inflightBufferLimit = maxInflightBuffers,
            .cpus = sourceDescriptor.runsOnPerformanceCpus() ? CpuPinning::Role::Performance : CpuPinning::Role::Misc,
            .rawTupleSize = rawTupleSize};

        return std::make_unique<SourceHandle>(
            std::move(backpressureListener), std::move(originId), std::move(runtimeConfig), bufferPool, std::move(source));
    }
    throw UnknownSourceType("unknown source descriptor type: {}", sourceDescriptor.getSourceType());
}

bool SourceProvider::contains(const std::string& sourceType) const ///NOLINT(readability-convert-member-functions-to-static)
{
    return SourceRegistry::instance().contains(sourceType);
}

}
