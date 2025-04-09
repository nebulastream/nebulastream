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

#include "RustCompressionSource.hpp"
#include <stop_token>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <fstream>
#include <string>
#include <unordered_map>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <Configurations/Descriptor.hpp>
#include "cxx.hpp"
#include "lib.rs.hpp"

namespace NES::Sources
{

    rust::Box<Rust::RustCompressionSourceImpl> createRustImpl(const SourceDescriptor& sourceDescriptor)
    {
        std::string s = static_cast<std::string>(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH));
        return Rust::new_rust_file_source(rust::Str(s.data(), s.size()));
    }

    RustCompressionSource::RustCompressionSource(const SourceDescriptor& sourceDescriptor)
    : impl(createRustImpl(sourceDescriptor))
    {
    }

    void RustCompressionSource::open()
    {
        this->impl->open();
    }

    void RustCompressionSource::close()
    {
        this->impl->close();
    }

    std::size_t RustCompressionSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
    {
        return this->impl->fill_tuple_buffer(tupleBuffer.getBuffer<uint8_t>(), tupleBuffer.getBufferSize());
    }

    NES::Configurations::DescriptorConfig::Config RustCompressionSource::validateAndFormat(
        std::unordered_map<std::string, std::string> config
    )
    {
        return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
    }

    std::ostream& RustCompressionSource::toString(std::ostream& str) const
    {
        rust::String rust_string = this->impl->to_string();
        std::string cpp_string(rust_string.c_str(), rust_string.length());
        Rust::free_string(rust_string);
        str << cpp_string;
        return str;
    }

    SourceValidationRegistryReturnType
    SourceValidationGeneratedRegistrar::RegisterRustCompressionSourceValidation(SourceValidationRegistryArguments sourceConfig)
    {
        return RustCompressionSource::validateAndFormat(std::move(sourceConfig.config));
    }

    SourceRegistryReturnType SourceGeneratedRegistrar::RegisterRustCompressionSource(SourceRegistryArguments sourceRegistryArguments)
    {
        return std::make_unique<RustCompressionSource>(sourceRegistryArguments.sourceDescriptor);
    }

}
