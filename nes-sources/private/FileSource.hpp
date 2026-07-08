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

#include <atomic>
#include <cstddef>
#include <fstream>
#include <memory>
#include <stop_token>
#include <string>
#include <string_view>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/FileSourceConfig.hpp>
#include <Sources/Source.hpp>

namespace NES
{

static const auto SYSTEST_FILE_PATH_PARAMETER = Identifier::parse("FILE_PATH");


class FileSource final : public Source
{
public:
    static constexpr std::string_view NAME = "File";

    explicit FileSource(const FileSourceConfig& config);
    ~FileSource() override = default;

    FileSource(const FileSource&) = delete;
    FileSource& operator=(const FileSource&) = delete;
    FileSource(FileSource&&) = delete;
    FileSource& operator=(FileSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open file socket.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close file socket.
    void close() override;

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

private:
    std::ifstream inputFile;
    std::string filePath;
    std::atomic<size_t> totalNumBytesRead;
};

}
