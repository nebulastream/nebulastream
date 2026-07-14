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
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>

namespace NES
{

struct Context;

class ODBCSource : public Source
{
public:
    static constexpr std::string_view NAME = "ODBC";

    explicit ODBCSource(const SourceDescriptor& sourceDescriptor);
    ~ODBCSource() override;

    ODBCSource(const ODBCSource&) = delete;
    ODBCSource& operator=(const ODBCSource&) = delete;
    ODBCSource(ODBCSource&&) = delete;
    ODBCSource& operator=(ODBCSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stoken) override;

    /// Open ODBC connection.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProviderPtr) override;
    /// Close ODBC connection.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] bool addsMetadata() const override { return false; }

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    size_t pollIntervalMs;
    std::string query;
    Schema schema;
    std::chrono::hours timezoneOffset;
    std::shared_ptr<AbstractBufferProvider> bufferProvider;

    std::unique_ptr<Context> sourceContext;
};

/// Defines the names, (optional) default values, (optional) validation & config functions, for all ODBC config parameters.
/// NOLINTBEGIN(cert-err58-cpp): static-storage ConfigParameter initialization is the project-wide pattern for source plugins.
/// The constructors can theoretically throw `std::bad_alloc`; in practice they are evaluated once at static-init time on a
/// path the runtime cannot meaningfully recover from. Refactoring would require redesigning the ConfigParameter registry
/// across every plugin — out of scope for any single PR.
struct ConfigParametersODBC
{
    /// Named "db_host"/"db_port" rather than "host"/"port" so that inline source
    /// SQL can still use `SOURCE.HOST` for worker placement (the inline-source
    /// binder unconditionally extracts the "host" config key as placement). Matches
    /// ODBCSink's db_host/db_port.
    static inline const DescriptorConfig::ConfigParameter<std::string> DB_HOST{
        "db_host",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DB_HOST, config); }};
    static inline const DescriptorConfig::ConfigParameter<std::string> DB_PORT{
        "db_port",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DB_PORT, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> DATABASE{
        "database",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DATABASE, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> USERNAME{
        "username",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(USERNAME, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> PASSWORD{
        "password",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PASSWORD, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> DRIVER{
        "driver",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        { return DescriptorConfig::tryGet(DRIVER, config); }};

    static inline const DescriptorConfig::ConfigParameter<size_t> POLL_INTERVAL_MS{
        "poll_interval_ms",
        1000,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<size_t>
        { return DescriptorConfig::tryGet(POLL_INTERVAL_MS, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> QUERY{
        "query",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        { return DescriptorConfig::tryGet(QUERY, config); }};

    /// Only consumed by the SQL Server dialect of the connection string (see ODBCSource.cpp):
    /// Microsoft's ODBC Driver 18 defaults to Encrypt=Mandatory, so connecting to a SQL Server
    /// with a self-signed certificate needs TrustServerCertificate=yes. Defaults to false so the
    /// psqlodbc path (which has no such keyword) is unaffected.
    static inline const DescriptorConfig::ConfigParameter<bool> TRUST_SERVER_CERTIFICATE{
        "trust_server_certificate",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TRUST_SERVER_CERTIFICATE, config); }};

    /// The windowed poller binds `now()` into the query's (watermark, now] bounds. `now()` is UTC,
    /// but a database that stores wall-clock local timestamps (e.g. the MLife tables in GMT+2) is
    /// then ahead of the window, so fresh rows are only picked up hours late. This shifts the bound
    /// timestamps by whole hours so they line up with the source column's timezone: set it to the
    /// column's UTC offset (e.g. 2 for GMT+2). Intended range [-24, 24]; validateAndFormat enforces it.
    static inline const DescriptorConfig::ConfigParameter<int32_t> TIMEZONE_OFFSET_HOURS{
        "timezone_offset_hours",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TIMEZONE_OFFSET_HOURS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap,
            DB_HOST,
            DB_PORT,
            DRIVER,
            POLL_INTERVAL_MS,
            QUERY,
            USERNAME,
            PASSWORD,
            DATABASE,
            TRUST_SERVER_CERTIFICATE,
            TIMEZONE_OFFSET_HOURS);
};

/// NOLINTEND(cert-err58-cpp)

}
