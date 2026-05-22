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

#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <sql.h>
#include <sqltypes.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that writes every incoming tuple as a row into a relational table
/// over ODBC. The buffer reaching execute() already holds the formatted output
/// (default `output_format=CSV`), so the sink splits it into rows and issues
/// one `INSERT INTO <table> VALUES(...)` per row with the field values quoted
/// as string literals — the target database coerces them to the column types.
/// This is the streaming counterpart of the ODBC source's ATTACH-time seeding
/// (ODBCTestLoader.cpp); it needs no NebulaStream-to-SQL type mapping of its
/// own. unixODBC is the driver manager; the runtime driver (e.g. psqlodbc) is
/// a system component registered in /etc/odbcinst.ini.
class ODBCSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "ODBC";

    explicit ODBCSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~ODBCSink() override;

    ODBCSink(const ODBCSink&) = delete;
    ODBCSink& operator=(const ODBCSink&) = delete;
    ODBCSink(ODBCSink&&) = delete;
    ODBCSink& operator=(ODBCSink&&) = delete;

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    /// Free every allocated ODBC handle in reverse order and reset them to
    /// null. Safe to call more than once; runs from stop(), the destructor,
    /// and the start() failure path so a half-open connection never leaks.
    void closeConnection() noexcept;

    std::string host;
    std::string port;
    std::string database;
    std::string username;
    std::string password;
    std::string driver;
    std::string table;

    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    size_t insertedRows{0};
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all ODBC sink config parameters.
/// NOTE: the connection-target keys are `db_host` / `db_port`, not `host` / `port`, for the same reason ODBCSource uses them:
/// the SQL binder reserves a bare `host` config key for worker placement and strips it before the sink ever sees it.
/// NOLINTBEGIN(cert-err58-cpp): static-storage ConfigParameter initialization is the project-wide pattern for sink plugins
/// (FileSink, etc.). The constructors can theoretically throw `std::bad_alloc`; in practice they are evaluated once at
/// static-init time on a path the runtime cannot meaningfully recover from. Refactoring would require redesigning the
/// ConfigParameter registry across every plugin — out of scope for any single PR.
struct ConfigParametersODBCSink
{
    static inline const DescriptorConfig::ConfigParameter<std::string> HOST{
        "db_host",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HOST, config); }};

    static inline const DescriptorConfig::ConfigParameter<std::string> PORT{
        "db_port",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(PORT, config); }};

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
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DRIVER, config); }};

    /// Target table the sink appends rows to. It must already exist with a
    /// column layout matching the query's output schema, in order.
    static inline const DescriptorConfig::ConfigParameter<std::string> TABLE{
        "table",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TABLE, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, HOST, PORT, DATABASE, USERNAME, PASSWORD, DRIVER, TABLE);
};

/// NOLINTEND(cert-err58-cpp)

}

FMT_OSTREAM(NES::ODBCSink);
