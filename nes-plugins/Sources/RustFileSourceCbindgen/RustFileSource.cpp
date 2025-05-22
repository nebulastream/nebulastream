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

#include "RustFileSource.hpp"
#include <stop_token>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <string>
#include <unordered_map>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <Configurations/Descriptor.hpp>

namespace NES::Sources
{
    static rust::RustFileSourceImpl* createRustImpl(const SourceDescriptor& sourceDescriptor)
    {
        std::string path = static_cast<std::string>(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH));
        return rust::new_rust_file_source((const uint8_t *) path.data(), path.size());
    }

    RustFileSource::RustFileSource(const SourceDescriptor& sourceDescriptor)
    : impl(createRustImpl(sourceDescriptor))
    {
    }

    void RustFileSource::open()
    {
        rust::open(this->impl);
    }

    void RustFileSource::close()
    {
        rust::close(this->impl);
    }

    std::size_t RustFileSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
    {
        return rust::fill_tuple_buffer(this->impl, tupleBuffer.getBuffer<uint8_t>(), tupleBuffer.getBufferSize());
    }

    NES::Configurations::DescriptorConfig::Config RustFileSource::validateAndFormat(
        std::unordered_map<std::string, std::string> config
    )
    {
        return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
    }

    std::ostream& RustFileSource::toString(std::ostream& str) const
    {
        str << "TODO toString implementieren";
        return str;
    }

    SourceValidationRegistryReturnType
    SourceValidationGeneratedRegistrar::RegisterRustFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
    {
        return RustFileSource::validateAndFormat(std::move(sourceConfig.config));
    }

    SourceRegistryReturnType SourceGeneratedRegistrar::RegisterRustFileSource(SourceRegistryArguments sourceRegistryArguments)
    {
        return std::make_unique<RustFileSource>(sourceRegistryArguments.sourceDescriptor);
    }

}
