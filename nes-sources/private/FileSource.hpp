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

#include <cstdint>
#include <optional>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/io_context.hpp>

#include <Sources/AsyncSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Configurations/Descriptor.hpp>
#include <Sources/AsyncSource.hpp>

namespace NES::Sources
{

class FileSource final : public AsyncSource
{
public:
    static inline const std::string NAME = "File";

    explicit FileSource(const SourceDescriptor& sourceDescriptor);
    ~FileSource() override = default;

    FileSource(const FileSource&) = delete;
    FileSource& operator=(const FileSource&) = delete;
    FileSource(FileSource&&) = delete;
    FileSource& operator=(FileSource&&) = delete;

    asio::awaitable<InternalSourceResult> fillBuffer(IOBuffer& buffer) override;

    /// Open file socket.
    asio::awaitable<void> open(asio::io_context& ioc) override;
    /// Close file socket.
    void close() override;

    /// Cancel operations on the file stream
    void cancel() override;

    /// validates and formats a string to string configuration
    static std::unique_ptr<Configurations::DescriptorConfig::Config> validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    const std::string filePath;
    std::optional<int32_t> fileDescriptor;
    std::optional<asio::posix::stream_descriptor> fileStream;
};

struct ConfigParametersFile
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> FILEPATH{
        "filePath",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(FILEPATH, config); }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(FILEPATH);
};

}
