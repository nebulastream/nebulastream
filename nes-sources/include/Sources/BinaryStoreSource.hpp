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

#pragma once

#include <fstream>
#include <string>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

/// Reads rows produced by Store from a binary file and fills TupleBuffers in row layout.
class BinaryStoreSource final : public Source
{
public:
    static constexpr std::string_view NAME = "BinaryStore";
    explicit BinaryStoreSource(const SourceDescriptor& sourceDescriptor);
    ~BinaryStoreSource() override = default;

    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    void close() override;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    [[nodiscard]] uint32_t getRowWidthBytes() const;

    /// Read the binary store file header and reconstruct the Schema from the embedded schema text.
    static Schema readSchemaFromFile(const std::string& filePath);

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    std::string filePath;
    std::ifstream inputFile;
    uint64_t dataStartOffset{0};
    std::atomic<uint64_t> totalNumBytesRead{0};
    Schema schema;
};

}
