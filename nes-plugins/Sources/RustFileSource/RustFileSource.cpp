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
#include <iostream>
#include <string>
#include <unordered_map>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <Configurations/Descriptor.hpp>
#include "cxx.hpp"
#include "lib.rs.hpp"

namespace NES::Sources
{
    static rust::Box<Rust::RustFileSourceImpl> createRustImpl(const SourceDescriptor& sourceDescriptor)
    {
        std::string path = static_cast<std::string>(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH));
        return Rust::new_rust_file_source(rust::Str(path.data(), path.size()));
    }

    RustFileSource::RustFileSource(const SourceDescriptor& sourceDescriptor)
    : impl(createRustImpl(sourceDescriptor))
    {
    }

    void RustFileSource::open()
    {
        this->impl->open();
    }

    void RustFileSource::close()
    {
        this->impl->close();
    }

    std::size_t RustFileSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
    {
        auto before = std::chrono::high_resolution_clock::now().time_since_epoch();
        auto result = this->impl->fill_tuple_buffer(tupleBuffer.getBuffer<uint8_t>(), tupleBuffer.getBufferSize());
        auto after = std::chrono::high_resolution_clock::now().time_since_epoch();
        auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(after - before).count();
        std::cout << "RustFileSource needed " << elapsed << "μs to fillTupleBuffer of "
                  << tupleBuffer.getBufferSize() << "bytes." << std::endl;
        return result;
    }

    NES::Configurations::DescriptorConfig::Config RustFileSource::validateAndFormat(
        std::unordered_map<std::string, std::string> config
    )
    {
        return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
    }

    std::ostream& RustFileSource::toString(std::ostream& str) const
    {
        rust::String rustString = this->impl->to_string();
        std::string cpp_string(rustString.c_str(), rustString.length());
        Rust::free_string(rustString);
        str << cpp_string;
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
