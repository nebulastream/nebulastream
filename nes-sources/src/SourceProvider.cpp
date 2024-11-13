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

#include <memory>

#include <API/AttributeField.hpp>
#include <SourceParsers/SourceParserCSV.hpp>
#include <Sources/SourceCSV.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Sources/SourceRegistry.hpp>
#include <Sources/SourceTCP.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Sources
{

std::unique_ptr<Sources::SourceProvider> SourceProvider::create()
{
    return std::make_unique<SourceProvider>();
}

std::unique_ptr<SourceHandle> SourceProvider::lower(
    OriginId originId,
    const SourceDescriptor& sourceDescriptor,
    std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool,
    SourceReturnType::EmitFunction&& emitFunction)
{
    /// Todo #241: Get the new source identfier from the source descriptor and pass it to SourceHandle.
    std::vector<PhysicalTypePtr> physicalTypes;
    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : sourceDescriptor.schema->fields)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }
    // Todo: sourceDescriptor.getFromConfig(ConfigParametersCSV::DELIMITER) fails silently/very badly, if the parameter does not exist
    // -> should use tryGetFromConfig
    std::string delimiter = ",";
    if(sourceDescriptor.sourceType == "CSV")
    {
        delimiter = sourceDescriptor.getFromConfig(ConfigParametersCSV::DELIMITER);
    }

    // Todo: remove hardcoded
    auto sourceParser = std::make_unique<SourceParsers::SourceParserCSV>(sourceDescriptor.schema, physicalTypes, delimiter);

    if (auto source = SourceRegistry::instance().create(sourceDescriptor.sourceType, sourceDescriptor))
    {
        return std::make_unique<SourceHandle>(
            std::move(originId),
            std::move(bufferPool),
            std::move(emitFunction),
            NUM_SOURCE_LOCAL_BUFFERS,
            std::move(source.value()),
            std::move(sourceParser));
    }
    throw UnknownSourceType("unknown source descriptor type: {}", sourceDescriptor.sourceType);
}

}
