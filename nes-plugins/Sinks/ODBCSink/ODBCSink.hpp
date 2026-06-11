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
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// State that depends on the ODBC C-API headers (the RAII connection + the
/// bound prepared INSERT). Defined in the .cpp so this header stays free of
/// `<sql.h>`; owned through a unique_ptr, mirroring ODBCSource's `Context`.
struct ODBCSinkContext;

/// A sink that writes every incoming tuple as a row into a relational table
/// over ODBC. The sink consumes the **native** tuple layout (it does not set an
/// `output_format`, so the engine hands it the native row buffer) together with
/// the output schema obtained from its SinkDescriptor. It prepares one
/// `INSERT INTO <table> VALUES (?, …, ?)` and binds each schema column to a
/// typed ODBC parameter via SQLBindParameter — the inverse of ODBCSource's
/// SQLBindCol fetch path (see ODBCSource.cpp / ODBCTypeMappings.hpp). No CSV
/// round-trip, no string-built SQL: values bind by type, NULLs round-trip as
/// SQL NULL, and a schema/table column mismatch fails fast in start().
/// unixODBC is the driver manager; the runtime driver (e.g. psqlodbc) is a
/// system component registered in /etc/odbcinst.ini.
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
    std::string host;
    std::string port;
    std::string database;
    std::string username;
    std::string password;
    std::string driver;
    std::string table;
    /// Output schema, from the SinkDescriptor — the sink binds its columns by
    /// type, so a column count/type mismatch with `table` fails in start().
    Schema schema;

    /// RAII connection + prepared INSERT; engaged by start(), reset by stop()
    /// and the destructor. The unique_ptr's deleters free the ODBC handles in
    /// reverse order, so there is no manual teardown path.
    std::unique_ptr<ODBCSinkContext> context;
    size_t insertedRows{0};
    /// Serializes execute(): the conn-test sink harness — and the engine — call execute()
    /// concurrently on one ODBCSink instance, but a single ODBC statement handle (and the
    /// insertedRows counter) must be touched by one thread at a time. One connection per
    /// sink is the deliberate model; thread-local connections are a future option, gated on
    /// a measured contention bottleneck (the source's fetch is single-threaded, so it has no
    /// shared-handle analog).
    std::mutex insertMutex;
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
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(DRIVER, config); }};

    /// Target table the sink appends rows to. It must already exist with a
    /// column layout matching the query's output schema, in order.
    static inline const DescriptorConfig::ConfigParameter<std::string> TABLE{
        "table",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(TABLE, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, DB_HOST, DB_PORT, DATABASE, USERNAME, PASSWORD, DRIVER, TABLE);
};

/// NOLINTEND(cert-err58-cpp)

}

FMT_OSTREAM(NES::ODBCSink);
