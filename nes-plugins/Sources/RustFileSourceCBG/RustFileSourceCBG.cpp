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

#include <stop_token>
#include <string>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include "RustFileSourceCBG.hpp"
#include <iostream>

namespace NES::Sources
{
    static rust::RustFileSourceImpl* createRustImpl(const SourceDescriptor& sourceDescriptor)
    {
        std::string path = static_cast<std::string>(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH));

        return rust::new_rust_file_source(path.data());
    }

    RustFileSourceCBG::RustFileSourceCBG(const SourceDescriptor& sourceDescriptor)
    : impl(createRustImpl(sourceDescriptor))
    {
    }

    RustFileSourceCBG::~RustFileSourceCBG()
    {
        rust::free_rust_file_source(this->impl);
    }

    void RustFileSourceCBG::open()
    {
        rust::openn(this->impl);
    }

    void RustFileSourceCBG::close()
    {
        rust::closee(this->impl);
    }

    std::size_t RustFileSourceCBG::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
    {
        return rust::fill_tuple_bufferr(this->impl, tupleBuffer.getBuffer<uint8_t>(), tupleBuffer.getBufferSize());
    }

    NES::Configurations::DescriptorConfig::Config RustFileSourceCBG::validateAndFormat(
        std::unordered_map<std::string, std::string> config
    )
    {
        return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
    }

    std::ostream& RustFileSourceCBG::toString(std::ostream& str) const
    {
        str << "RustFileSourceCBG";
        return str;
    }

    SourceValidationRegistryReturnType
    SourceValidationGeneratedRegistrar::RegisterRustFileCBGSourceValidation(SourceValidationRegistryArguments sourceConfig)
    {
        return RustFileSourceCBG::validateAndFormat(std::move(sourceConfig.config));
    }

    SourceRegistryReturnType SourceGeneratedRegistrar::RegisterRustFileCBGSource(SourceRegistryArguments sourceRegistryArguments)
    {
        return std::make_unique<RustFileSourceCBG>(sourceRegistryArguments.sourceDescriptor);
    }

    InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterRustFileCBGInlineData(InlineDataRegistryArguments systestAdaptorArguments)
    {
        if (systestAdaptorArguments.attachSource.tuples)
        {
            if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(std::string(SYSTEST_FILE_PATH_PARAMETER));
                filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                filePath->second = systestAdaptorArguments.testFilePath;
                if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
                {
                    /// Write inline tuples to test file.
                    for (const auto& tuple : systestAdaptorArguments.attachSource.tuples.value())
                    {
                        testFile << tuple << "\n";
                    }
                    testFile.flush();
                    return systestAdaptorArguments.physicalSourceConfig;
                }
                throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
            }
            throw InvalidConfigParameter("A RustFileSourceCBG config must contain filePath parameter");
        }
        throw TestException("An INLINE SystestAttachSource must not have a 'tuples' vector that is null.");
    }

    FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterRustFileCBGFileData(FileDataRegistryArguments systestAdaptorArguments)
    {
        /// Check that the test data dir is defined and that the 'filePath' parameter is set
        /// Replace the 'TESTDATA' placeholder in the filepath
        if (const auto attachSourceFilePath = systestAdaptorArguments.attachSource.fileDataPath)
        {
            if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(std::string(SYSTEST_FILE_PATH_PARAMETER));
                filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                filePath->second = attachSourceFilePath.value();
                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw InvalidConfigParameter("A RustFileSourceCBG config must contain filePath parameter.");
        }
        throw InvalidConfigParameter("An attach source of type FileData must contain a filePath configuration.");
    }

}
