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

#include "RustChecksumSink.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{
    static rust::Box<Rust::ChecksumSink> createRustImpl(const SinkDescriptor& sinkDescriptor)
    {
        std::string path = sinkDescriptor.getFromConfig(ConfigParametersChecksum::FILEPATH);
        return Rust::new_rust_checksum_sink(rust::Str(path.data(), path.size()));
    }

    RustChecksumSink::RustChecksumSink(const SinkDescriptor& sinkDescriptor)
        : impl(createRustImpl(sinkDescriptor)),
        formatter(std::make_unique<CSVFormat>(sinkDescriptor.schema))
    {
    }

    void RustChecksumSink::start(Runtime::Execution::PipelineExecutionContext&)
    {
        this->impl->start();
    }

    void RustChecksumSink::stop(Runtime::Execution::PipelineExecutionContext&)
    {
        this->impl->stop();
    }

    void RustChecksumSink::execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext&)
    {
        const std::string formatted = formatter->getFormattedBuffer(inputBuffer);
        this->impl->execute(rust::Str(formatted.data(), formatted.size()))
    }

    Configurations::DescriptorConfig::Config RustChecksumSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
    {
        return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersChecksum>(std::move(config), NAME);
    }

    SinkValidationRegistryReturnType
    SinkValidationGeneratedRegistrar::RegisterRustChecksumSinkValidation(SinkValidationRegistryArguments sinkConfig)
    {
        return RustChecksumSink::validateAndFormat(std::move(sinkConfig.config));
    }

    SinkRegistryReturnType SinkGeneratedRegistrar::RegisterRustChecksumSink(SinkRegistryArguments sinkRegistryArguments)
    {
        return std::make_unique<RustChecksumSink>(sinkRegistryArguments.sinkDescriptor);
    }

}
