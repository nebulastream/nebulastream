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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/types.h>
#include "Configurations/Descriptor.hpp"
#include "Sources/SourceDescriptor.hpp"


namespace NES::Sources
{

struct Context;

class ODBCSource : public Source
{
    constexpr static std::chrono::microseconds ODBC_SOCKET_DEFAULT_TIMEOUT{100000};
    constexpr static ssize_t INVALID_RECEIVED_BUFFER_SIZE = -1;
    /// A return value of '0' means an EoF in the context of a read(socket..) (https://man.archlinux.org/man/core/man-pages/read.2.en)
    constexpr static ssize_t EOF_RECEIVED_BUFFER_SIZE = 0;

public:
    static constexpr std::string_view NAME = "ODBC";

    explicit ODBCSource(const SourceDescriptor& sourceDescriptor);
    ~ODBCSource() override;

    ODBCSource(const ODBCSource&) = delete;
    ODBCSource& operator=(const ODBCSource&) = delete;
    ODBCSource(ODBCSource&&) = delete;
    ODBCSource& operator=(ODBCSource&&) = delete;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open ODBC connection.
    void open() override;
    /// Close ODBC connection.
    void close() override;

    static std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
    validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string host;
    std::string database;
    std::string username;
    std::string password;
    std::string driver;
    std::string query;

    std::unique_ptr<Context> context;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
};


/// Defines the names, (optional) default values, (optional) validation & config functions, for all ODBC config parameters.
struct ConfigParametersODBC
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> HOST{
        "host", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(HOST, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> DATABASE{
        "database", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(DATABASE, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> USERNAME{
        "username", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(USERNAME, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> PASSWORD{
        "password", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return Configurations::DescriptorConfig::tryGet(PASSWORD, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> DRIVER{
        "driver", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string> {
            return Configurations::DescriptorConfig::tryGet(DRIVER, config);
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> QUERY{
        "query", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string> {
            return Configurations::DescriptorConfig::tryGet(QUERY, config);
        }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(HOST, DRIVER, QUERY, USERNAME, PASSWORD, DATABASE);
};

}
