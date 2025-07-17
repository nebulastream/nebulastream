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

#include "RustFileSinkCXX.hpp"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>
#include <cxx.hpp>
#include <lib.rs.hpp>

namespace NES::Sinks
{
    static rust::Box<Rust::FileSinkImpl> createRustImpl(const SinkDescriptor& sinkDescriptor)
    {
        std::string path = sinkDescriptor.getFromConfig(ConfigParametersFile::FILEPATH);
        bool append = sinkDescriptor.getFromConfig(ConfigParametersFile::APPEND);
        return Rust::new_file_sink(rust::Str(path.data(), path.size()), append);
    }

    RustFileSinkCXX::RustFileSinkCXX(const SinkDescriptor& sinkDescriptor)
        : Sink()
        , impl(createRustImpl(sinkDescriptor))
    {
        switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersFile::INPUT_FORMAT))
        {
            case Configurations::InputFormat::CSV:
                formatter = std::make_unique<CSVFormat>(sinkDescriptor.schema);
                break;
            default:
                throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
        }
    }

    std::ostream& RustFileSinkCXX::toString(std::ostream& str) const
    {
        str << fmt::format("RustFileSinkCXX");
        return str;
    }

    void RustFileSinkCXX::start(PipelineExecutionContext&)
    {
        const auto schemaStr = formatter->getFormattedSchema();
        this->impl->start(rust::Str(schemaStr.data(), schemaStr.size()));
    }

    void RustFileSinkCXX::execute(const Memory::TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
    {
        auto fBuffer = formatter->getFormattedBuffer(inputTupleBuffer);
        this->impl->execute(rust::Str(fBuffer.data(), fBuffer.size()));
    }

    void RustFileSinkCXX::stop(PipelineExecutionContext&)
    {
        this->impl->stop();
    }

    Configurations::DescriptorConfig::Config RustFileSinkCXX::validateAndFormat(std::unordered_map<std::string, std::string> config)
    {
        return NES::Configurations::DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(config), NAME);
    }

    SinkValidationRegistryReturnType SinkValidationGeneratedRegistrar::RegisterRustFileSinkCXXSinkValidation(SinkValidationRegistryArguments sinkConfig)
    {
        return RustFileSinkCXX::validateAndFormat(std::move(sinkConfig.config));
    }

    SinkRegistryReturnType SinkGeneratedRegistrar::RegisterRustFileSinkCXXSink(SinkRegistryArguments sinkRegistryArguments)
    {
        return std::make_unique<RustFileSinkCXX>(sinkRegistryArguments.sinkDescriptor);
    }

}
