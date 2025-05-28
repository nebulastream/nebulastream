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
        std::cout << "Casting string \"" << path << " to byte pointer." << std::endl;
        auto data = reinterpret_cast<const unsigned char*>(path.data());
        std::cout << "After Karsten" << std::endl;
        return rust::new_rust_file_source(data, path.size());
    }

    RustFileSourceCBG::RustFileSourceCBG(const SourceDescriptor& sourceDescriptor)
    : impl(createRustImpl(sourceDescriptor))
    {
    }

    void RustFileSourceCBG::open()
    {
        std::cout << "HUGELBUGEL\topen" << std::endl;
        rust::open(this->impl);
    }

    void RustFileSourceCBG::close()
    {
        std::cout << "HUGELBUGEL\tclose" << std::endl;
        rust::close(this->impl);
    }

    std::size_t RustFileSourceCBG::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
    {
        std::cout << "HUGELBUGEL\tfillTupleBuffer" << std::endl;
        return rust::fill_tuple_buffer(this->impl, tupleBuffer.getBuffer<uint8_t>(), tupleBuffer.getBufferSize());
    }

    NES::Configurations::DescriptorConfig::Config RustFileSourceCBG::validateAndFormat(
        std::unordered_map<std::string, std::string> config
    )
    {
        std::cout << "HUGELBUGEL\tvalidateAndFormat" << std::endl;
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
        std::cout << "HUGELBUGEL\tRegisterRustFileCBGSourceValidation" << std::endl;
        return RustFileSourceCBG::validateAndFormat(std::move(sourceConfig.config));
    }

    SourceRegistryReturnType SourceGeneratedRegistrar::RegisterRustFileCBGSource(SourceRegistryArguments sourceRegistryArguments)
    {
        std::cout << "HUGELBUGEL\tRegisterRustFileCBGSource" << std::endl;
        return std::make_unique<RustFileSourceCBG>(sourceRegistryArguments.sourceDescriptor);
    }

    InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterRustFileCBGInlineData(InlineDataRegistryArguments systestAdaptorArguments)
    {
        std::cout << "HUGELBUGEL\tRegisterRustFileCBGInlineData" << std::endl;
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
        std::cout << "HUGELBUGEL\tRegisterRustFileCBGFileData" << std::endl;
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
