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
#include <string>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <RustChecksumSinkCBG.hpp>

namespace NES::Sinks
{
    static rust::RustChecksumSinkImpl* createRustImpl(const SinkDescriptor& sinkDescriptor)
    {
        std::string path = sinkDescriptor.getFromConfig(ConfigParametersRustChecksum::FILEPATH);
        return rust::new_rust_checksum_sink(path.data());
    }

    RustChecksumSinkCBG::RustChecksumSinkCBG(const SinkDescriptor& sinkDescriptor)
        : impl(createRustImpl(sinkDescriptor)),
        formatter(std::make_unique<CSVFormat>(sinkDescriptor.schema))
    {
    }

    void RustChecksumSinkCBG::start(PipelineExecutionContext&)
    {
        rust::start_rust_checksum_sink(this->impl);
    }

    void RustChecksumSinkCBG::stop(PipelineExecutionContext&)
    {
        rust::stop_rust_checksum_sink(this->impl);
    }

    void RustChecksumSinkCBG::execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&)
    {
        const std::string formatted = formatter->getFormattedBuffer(inputBuffer);
        rust::execute_rust_checksum_sink(this->impl, formatted.data());
    }

    Configurations::DescriptorConfig::Config RustChecksumSinkCBG::validateAndFormat(std::unordered_map<std::string, std::string> config)
    {
        return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersRustChecksum>(std::move(config), NAME);
    }

    SinkValidationRegistryReturnType
    SinkValidationGeneratedRegistrar::RegisterRustChecksumCBGSinkValidation(SinkValidationRegistryArguments sinkConfig)
    {
        return RustChecksumSinkCBG::validateAndFormat(std::move(sinkConfig.config));
    }

    SinkRegistryReturnType SinkGeneratedRegistrar::RegisterRustChecksumCBGSink(SinkRegistryArguments sinkRegistryArguments)
    {
        return std::make_unique<RustChecksumSinkCBG>(sinkRegistryArguments.sinkDescriptor);
    }
}
