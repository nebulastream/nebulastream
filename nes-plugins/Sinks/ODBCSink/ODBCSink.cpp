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

#include <ODBCSink.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

/// The ODBC C API speaks in `SQLCHAR*` (an unsigned-char alias) and binds
/// input parameters through a `void*`/`SQLPOINTER`. Bridging NebulaStream's
/// `std::byte` / `char` buffers to it unavoidably requires reinterpret_cast
/// across that C-API boundary. The check is silenced file-wide for it; every
/// cast here is mandated by the unixODBC headers.
///
/// `rc` (SQLRETURN) is the unixODBC convention at every API call site; the
/// readability-identifier-length band keeps grep-ability for ODBC-versed readers.
/// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length)
namespace NES
{

namespace
{

/// SQLSTATE is a fixed 5-character code; the buffer holds 5 chars + NUL terminator.
constexpr SQLSMALLINT SQL_STATE_BUFFER_SIZE = 6;
/// Buffer size for the connection string the driver echoes back from SQLDriverConnect.
constexpr SQLSMALLINT OUT_CONNECTION_STRING_SIZE = 1024;

/// Collect every ODBC diagnostic record attached to a handle into one string.
/// A single diagnostics path for the whole sink — the inverse of ODBCSource's
/// `showError` (this sink keeps its own copy rather than sharing a core).
std::string showError(SQLSMALLINT handleType, SQLHANDLE handle, const std::string& context)
{
    std::array<SQLCHAR, SQL_STATE_BUFFER_SIZE> sqlState{};
    std::array<SQLCHAR, SQL_MAX_MESSAGE_LENGTH> message{};
    SQLINTEGER nativeError = 0;
    SQLSMALLINT messageLen = 0;
    SQLSMALLINT recNum = 1;

    std::stringstream errorMessage;
    errorMessage << "Error in " << context << ":\n";
    while (SQLGetDiagRec(
               handleType,
               handle,
               recNum,
               sqlState.data(),
               &nativeError,
               message.data(),
               static_cast<SQLSMALLINT>(message.size()),
               &messageLen)
           == SQL_SUCCESS)
    {
        errorMessage << "  [" << reinterpret_cast<const char*>(sqlState.data()) << "] (" << nativeError << ") "
                     << reinterpret_cast<const char*>(message.data()) << "\n";
        ++recNum;
    }
    return errorMessage.str();
}

/// Throw `Exc` on a hard ODBC error, warn on a non-success info return.
/// Hardened to a `do { } while (0)` (the source's bare `if` dangles before an
/// `else`) and parameterized by exception type: open-time failures raise
/// `CannotOpenSink` (4004), ingest-time failures `RunningRoutineFailure` (4001),
/// matching the error-code contract the conn-test battery asserts.
/// A constexpr template can't replicate this: the exception type is a template
/// parameter at the throw site, and `throw Exc(...)` needs the caller's source
/// location for the exception's stack trace.
/// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define CHECK_THROW(Exc, rc, handleType, handle, context) \
    do \
    { \
        const SQLRETURN check_rc = (rc); \
        if (check_rc != SQL_SUCCESS && check_rc != SQL_SUCCESS_WITH_INFO) \
        { \
            if (check_rc == SQL_ERROR) \
            { \
                throw Exc("{}: {}", (context), showError((handleType), (handle), (context))); \
            } \
            NES_WARNING("{}:{}", (context), showError((handleType), (handle), (context))); \
        } \
    } while (0)

#define CHECK_SINK(rc, handleType, handle, context) CHECK_THROW(CannotOpenSink, (rc), (handleType), (handle), (context))
#define CHECK_RUNTIME(rc, handleType, handle, context) CHECK_THROW(RunningRoutineFailure, (rc), (handleType), (handle), (context))

/// NOLINTEND(cppcoreguidelines-macro-usage)

struct SqlStmtDeleter
{
    void operator()(SQLHSTMT handle) const noexcept
    {
        if (handle != SQL_NULL_HSTMT)
        {
            SQLFreeHandle(SQL_HANDLE_STMT, handle);
        }
    }
};

using StmtHandle = std::unique_ptr<void, SqlStmtDeleter>;

struct SqlConnectionDeleter
{
    void operator()(SQLHDBC handle) const noexcept
    {
        if (handle != SQL_NULL_HDBC)
        {
            SQLDisconnect(handle);
            SQLFreeHandle(SQL_HANDLE_DBC, handle);
        }
    }
};

using ConnectionHandle = std::unique_ptr<void, SqlConnectionDeleter>;

struct SqlEnvironmentDeleter
{
    void operator()(SQLHENV handle) const noexcept
    {
        if (handle != SQL_NULL_HENV)
        {
            SQLFreeHandle(SQL_HANDLE_ENV, handle);
        }
    }
};

using EnvironmentHandle = std::unique_ptr<void, SqlEnvironmentDeleter>;

struct ConnectionInformation
{
    std::string server;
    std::string port;
    std::string database;
    std::string username;
    std::string password;
    std::string driver;
};

/// Owns the env + dbc handles. Member order (env first) gives the correct
/// teardown order: the dbc is disconnected/freed before the env.
struct Connection
{
    EnvironmentHandle env;
    ConnectionHandle connection;

    explicit Connection(const ConnectionInformation& info)
    {
        /// `SERVER=<host>;PORT=<port>` — NOT `SERVER=<host>,<port>`. psqlodbc/libpq read a comma in
        /// SERVER as a multi-host list and ignore the PORT, so `SERVER=127.0.0.1,1` becomes hosts
        /// ["127.0.0.1", "1"] at the default port 5432 — and "1" parses as 0.0.0.1, whose connect
        /// hangs where the network drops the SYN (e.g. CI) because there is no login timeout. The
        /// semicolon form binds the real host:port and fails fast on a refused/closed port. Same
        /// layout as ODBCSource's Connection, so the sink reaches a database addressed the way the
        /// source does.
        auto connectionString = fmt::format(
            "DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};PWD={};",
            info.driver,
            info.server,
            info.port,
            info.database,
            info.username,
            info.password);
        NES_DEBUG("ODBCSink connecting to database: {}", connectionString);

        {
            SQLHANDLE tmp = SQL_NULL_HENV;
            CHECK_SINK(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &tmp), SQL_HANDLE_ENV, tmp, "SQLAllocHandle ENV");
            this->env = EnvironmentHandle{tmp};
        }
        SQLSetEnvAttr(env.get(), SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
        {
            SQLHANDLE tmp = SQL_NULL_HDBC;
            CHECK_SINK(SQLAllocHandle(SQL_HANDLE_DBC, env.get(), &tmp), SQL_HANDLE_DBC, tmp, "SQLAllocHandle DBC");
            this->connection = ConnectionHandle{tmp};
        }

        std::array<SQLCHAR, OUT_CONNECTION_STRING_SIZE> outConnStr{};
        SQLSMALLINT outConnStrLen = 0;
        const auto rc = SQLDriverConnect(
            connection.get(),
            nullptr,
            reinterpret_cast<SQLCHAR*>(connectionString.data()),
            SQL_NTS,
            outConnStr.data(),
            static_cast<SQLSMALLINT>(outConnStr.size()),
            &outConnStrLen,
            SQL_DRIVER_NOPROMPT);
        CHECK_SINK(rc, SQL_HANDLE_DBC, connection.get(), "SQLDriverConnect");
    }
};

/// How a NES field binds as an ODBC parameter. The inverse of ODBCTypeMappings'
/// read table; the read table is many-to-one (many SQL types → one NES type),
/// so it cannot be reversed mechanically — the sink keys on the NES type to
/// pick the C type and a default target SQL type, and lets the database coerce
/// to the column's real type. Timestamp columns (a UINT64 NES field written to
/// a SQL TIMESTAMP) are out of scope here — that windowed direction is owned by
/// the source.
struct SinkColumnType
{
    SQLSMALLINT cType;
    SQLSMALLINT sqlType;
    bool isVarSized;
};

std::optional<SinkColumnType> nesToSql(DataType::Type type)
{
    switch (type)
    {
        case DataType::Type::INT8:
            return SinkColumnType{.cType = SQL_C_STINYINT, .sqlType = SQL_TINYINT, .isVarSized = false};
        case DataType::Type::INT16:
            return SinkColumnType{.cType = SQL_C_SSHORT, .sqlType = SQL_SMALLINT, .isVarSized = false};
        case DataType::Type::INT32:
            return SinkColumnType{.cType = SQL_C_SLONG, .sqlType = SQL_INTEGER, .isVarSized = false};
        case DataType::Type::INT64:
            return SinkColumnType{.cType = SQL_C_SBIGINT, .sqlType = SQL_BIGINT, .isVarSized = false};
        case DataType::Type::UINT8:
            return SinkColumnType{.cType = SQL_C_UTINYINT, .sqlType = SQL_TINYINT, .isVarSized = false};
        case DataType::Type::UINT16:
            return SinkColumnType{.cType = SQL_C_USHORT, .sqlType = SQL_SMALLINT, .isVarSized = false};
        case DataType::Type::UINT32:
            return SinkColumnType{.cType = SQL_C_ULONG, .sqlType = SQL_INTEGER, .isVarSized = false};
        case DataType::Type::UINT64:
            return SinkColumnType{.cType = SQL_C_UBIGINT, .sqlType = SQL_BIGINT, .isVarSized = false};
        case DataType::Type::FLOAT32:
            return SinkColumnType{.cType = SQL_C_FLOAT, .sqlType = SQL_REAL, .isVarSized = false};
        case DataType::Type::FLOAT64:
            return SinkColumnType{.cType = SQL_C_DOUBLE, .sqlType = SQL_DOUBLE, .isVarSized = false};
        case DataType::Type::BOOLEAN:
            return SinkColumnType{.cType = SQL_C_BIT, .sqlType = SQL_BIT, .isVarSized = false};
        case DataType::Type::CHAR:
            return SinkColumnType{.cType = SQL_C_CHAR, .sqlType = SQL_CHAR, .isVarSized = false};
        case DataType::Type::VARSIZED:
            return SinkColumnType{.cType = SQL_C_CHAR, .sqlType = SQL_VARCHAR, .isVarSized = true};
        case DataType::Type::UNDEFINED:
            return std::nullopt;
    }
    return std::nullopt;
}

/// A prepared `INSERT INTO <table> VALUES (?, …, ?)` plus the per-column staging
/// it binds into — the sink's analog of ODBCSource's `PreparedStatement`, with
/// SQLBindParameter where the source uses SQLBindCol.
struct BoundInsert
{
    struct Column
    {
        size_t offset{0}; ///< byte offset of the field within a native tuple
        size_t rawSize{0}; ///< value size without the null byte
        bool nullable{false};
        bool isVarSized{false};
        SQLSMALLINT cType{0};
        SQLSMALLINT sqlType{0};
        /// Stable buffer the parameter points at across every SQLExecute (the
        /// pointer must outlive each call). For a varsized column this is just a
        /// 1-byte dummy used for the NULL case; the non-null value is re-bound
        /// per row directly from the input buffer's child bytes.
        std::vector<std::byte> scalarStage;
        /// Length / null indicator, bound by address and refreshed per row.
        SQLLEN strLenOrInd{0};
    };

    StmtHandle statement;
    std::vector<Column> columns;
    size_t tupleStride{0};

    /// Prepare + bind. Fail-fast: an unmappable NES type throws here (in
    /// start()), and the prepared INSERT's placeholder count is validated by
    /// the database at SQLPrepare against the target table.
    static BoundInsert bind(const Connection& connection, const Schema& schema, std::string_view table)
    {
        SQLHSTMT hStmt = SQL_NULL_HSTMT;
        CHECK_SINK(
            SQLAllocHandle(SQL_HANDLE_STMT, connection.connection.get(), &hStmt),
            SQL_HANDLE_DBC,
            connection.connection.get(),
            "SQLAllocHandle STMT");
        StmtHandle statement{hStmt};

        BoundInsert bound;
        bound.statement = std::move(statement);
        bound.tupleStride = schema.getSizeOfSchemaInBytes();
        bound.columns.reserve(schema.getNumberOfFields());

        std::string placeholders;
        size_t offset = 0;
        for (const auto& field : schema)
        {
            const auto& dataType = field.dataType;
            const auto mapping = nesToSql(dataType.type);
            if (!mapping)
            {
                throw CannotOpenSink(
                    "ODBCSink: cannot bind field `{}` of type {} to an ODBC parameter", field.name, static_cast<int>(dataType.type));
            }

            Column column;
            column.offset = offset;
            column.rawSize = dataType.getSizeInBytesWithoutNull();
            column.nullable = dataType.nullable;
            column.isVarSized = mapping->isVarSized;
            column.cType = mapping->cType;
            column.sqlType = mapping->sqlType;
            column.scalarStage.resize(column.isVarSized ? 1 : column.rawSize);
            bound.columns.push_back(std::move(column));

            offset += dataType.getSizeInBytesWithNull();
            placeholders += placeholders.empty() ? "?" : ", ?";
        }

        auto insert = fmt::format("INSERT INTO {} VALUES ({})", table, placeholders);
        NES_DEBUG("ODBCSink prepared: {}", insert);
        CHECK_SINK(SQLPrepare(hStmt, reinterpret_cast<SQLCHAR*>(insert.data()), SQL_NTS), SQL_HANDLE_STMT, hStmt, "SQLPrepare INSERT");

        /// Bind the fixed-width columns once to their stable staging buffers.
        /// Varsized columns are (re)bound per row in execute(), so only their
        /// NULL indicator address needs to be wired up front.
        for (SQLSMALLINT i = 0; std::cmp_less(i, bound.columns.size()); ++i)
        {
            auto& column = bound.columns[i];
            const SQLUSMALLINT param = i + 1;
            if (column.isVarSized)
            {
                continue;
            }
            CHECK_SINK(
                SQLBindParameter(
                    hStmt,
                    param,
                    SQL_PARAM_INPUT,
                    column.cType,
                    column.sqlType,
                    0,
                    0,
                    column.scalarStage.data(),
                    static_cast<SQLLEN>(column.rawSize),
                    &column.strLenOrInd),
                SQL_HANDLE_STMT,
                hStmt,
                "SQLBindParameter");
        }
        return bound;
    }

    /// Write every tuple of `buffer` (native layout) as one INSERT. Returns the
    /// number of rows inserted. Caller serializes — the staging buffers and the
    /// statement handle are shared mutable state.
    /// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic): native tuple layout is byte-offset addressed; a span::subspan rewrite would obscure the per-field walk.
    size_t execute(const TupleBuffer& buffer)
    {
        const auto* base = buffer.getAvailableMemoryArea<const std::byte>().data();
        const auto numTuples = buffer.getNumberOfTuples();
        for (uint64_t tupleIdx = 0; tupleIdx < numTuples; ++tupleIdx)
        {
            const auto* tuple = base + (tupleIdx * tupleStride);
            /// Child buffers backing this row's varsized values must stay alive
            /// until SQLExecute returns (the parameters point straight at them).
            std::vector<TupleBuffer> liveChildren;
            for (SQLUSMALLINT i = 0; std::cmp_less(i, columns.size()); ++i)
            {
                auto& column = columns[i];
                const SQLUSMALLINT param = i + 1;
                const auto* field = tuple + column.offset;
                const bool isNull = column.nullable && (*field == std::byte{1});
                const auto* value = field + (column.nullable ? 1 : 0);

                if (column.isVarSized)
                {
                    if (isNull)
                    {
                        column.strLenOrInd = SQL_NULL_DATA;
                        CHECK_RUNTIME(
                            SQLBindParameter(
                                statement.get(),
                                param,
                                SQL_PARAM_INPUT,
                                column.cType,
                                column.sqlType,
                                1,
                                0,
                                column.scalarStage.data(),
                                0,
                                &column.strLenOrInd),
                            SQL_HANDLE_STMT,
                            statement.get(),
                            "SQLBindParameter varsized null");
                        continue;
                    }
                    VariableSizedAccess access;
                    std::memcpy(&access, value, sizeof(VariableSizedAccess));
                    TupleBuffer child = buffer.loadChildBuffer(access.getIndex());
                    const auto bytes = child.getAvailableMemoryArea<const std::byte>().subspan(
                        access.getOffset().getRawOffset(), access.getSize().getRawSize());
                    column.strLenOrInd = static_cast<SQLLEN>(bytes.size());
                    CHECK_RUNTIME(
                        SQLBindParameter(
                            statement.get(),
                            param,
                            SQL_PARAM_INPUT,
                            column.cType,
                            column.sqlType,
                            bytes.empty() ? 1 : bytes.size(),
                            0,
                            /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast): SQLBindParameter takes a SQLPOINTER (void*); with SQL_PARAM_INPUT the driver only reads, so dropping const here is safe and unavoidable at the C-API boundary.
                            const_cast<std::byte*>(bytes.data()),
                            static_cast<SQLLEN>(bytes.size()),
                            &column.strLenOrInd),
                        SQL_HANDLE_STMT,
                        statement.get(),
                        "SQLBindParameter varsized");
                    liveChildren.push_back(std::move(child));
                    continue;
                }

                if (isNull)
                {
                    column.strLenOrInd = SQL_NULL_DATA;
                    continue;
                }
                std::memcpy(column.scalarStage.data(), value, column.rawSize);
                column.strLenOrInd = static_cast<SQLLEN>(column.rawSize);
            }
            CHECK_RUNTIME(SQLExecute(statement.get()), SQL_HANDLE_STMT, statement.get(), "SQLExecute");
        }
        return numTuples;
    }

    /// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
};

}

/// RAII connection + prepared INSERT. `connection` is declared before `insert`
/// so `insert` (the statement handle) is freed before the connection.
struct ODBCSinkContext
{
    Connection connection;
    BoundInsert insert;
};

ODBCSink::ODBCSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , host(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::HOST))
    , port(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::PORT))
    , database(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::DATABASE))
    , username(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::USERNAME))
    , password(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::PASSWORD))
    , driver(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::DRIVER))
    , table(sinkDescriptor.getFromConfig(ConfigParametersODBCSink::TABLE))
    , schema(*sinkDescriptor.getSchema())
{
}

ODBCSink::~ODBCSink() = default;

std::ostream& ODBCSink::toString(std::ostream& os) const
{
    os << "\nODBCSink(";
    os << "\n  host: " << host;
    os << "\n  port: " << port;
    os << "\n  database: " << database;
    os << "\n  table: " << table;
    os << "\n  insertedRows: " << insertedRows;
    os << ")\n";
    return os;
}

void ODBCSink::start(PipelineExecutionContext&)
{
    NES_INFO("Opening ODBCSink on table `{}` of database `{}` at {}:{}.", table, database, host, port);
    /// A throw partway through bind() frees whatever handles were allocated via
    /// the RAII deleters as the local Connection/BoundInsert unwind, so there is
    /// no half-open connection to clean up by hand.
    const ConnectionInformation info{
        .server = host, .port = port, .database = database, .username = username, .password = password, .driver = driver};
    Connection connection{info};
    auto insert = BoundInsert::bind(connection, schema, table);
    context = std::make_unique<ODBCSinkContext>(ODBCSinkContext{.connection = std::move(connection), .insert = std::move(insert)});
    NES_DEBUG("ODBCSink connected and prepared INSERT into `{}`.", table);
}

void ODBCSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(context != nullptr, "ODBCSink is not started");
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in ODBCSink.");

    /// execute() is called concurrently by many worker threads on one ODBCSink instance, but the
    /// single statement handle, the bound staging buffers, and the insertedRows counter must be
    /// touched by one thread at a time. One connection + one lock per sink is the deliberate model.
    const std::scoped_lock lock(insertMutex);
    insertedRows += context->insert.execute(inputTupleBuffer);
}

void ODBCSink::stop(PipelineExecutionContext&)
{
    context.reset();
    NES_INFO("ODBCSink completed: inserted {} row(s) into `{}`.", insertedRows, table);
}

DescriptorConfig::Config ODBCSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersODBCSink>(std::move(config), NAME);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkValidationRegistryReturnType RegisterODBCSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return ODBCSink::validateAndFormat(std::move(sinkConfig.config));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkRegistryReturnType RegisterODBCSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<ODBCSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length)
