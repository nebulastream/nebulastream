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
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <variant>

#include <Configurations/ConfigField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/FileSourceConfig.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

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

    /// Open a file or connect to an existing Unix-domain socket.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close the active file or socket.
    void close() override;


    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    struct SocketInput
    {
        explicit SocketInput(const std::string& path);
        ~SocketInput();

        SocketInput(const SocketInput&) = delete;
        SocketInput& operator=(const SocketInput&) = delete;
        SocketInput(SocketInput&&) = delete;
        SocketInput& operator=(SocketInput&&) = delete;

        int descriptor = -1;
    };

    /// Only the source thread accesses the input; monostate means unopened/closed.
    std::variant<std::monostate, std::ifstream, SocketInput> input;
    std::string filePath;
    std::atomic<size_t> totalNumBytesRead{0};
};

}
