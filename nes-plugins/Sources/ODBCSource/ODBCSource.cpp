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

#include <ODBCSource.hpp>

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <ostream>
#include <ranges>
#include <span>
#include <sstream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <ODBCTypeMappings.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

/// The ODBC C API speaks in `SQLCHAR*` (an unsigned-char alias) and binds parameters / fetches
/// columns through a `void*`/`SQLPOINTER`. Bridging NebulaStream's `std::byte` / `char` buffers
/// to it unavoidably requires reinterpret_cast across that C-API boundary. The check is silenced
/// file-wide for it; every cast here is mandated by the unixODBC headers.
///
/// `rc` (SQLRETURN) is the unixODBC convention and appears at every API call; the
/// readability-identifier-length band keeps grep-ability for ODBC-versed readers.
/// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length)
namespace NES
{

struct ConnectionInformation
{
    std::string server;
    std::string port;
    std::string database;
    std::string username;
    std::string password;
    std::string driver;
};

namespace
{

/// SQLSTATE is a fixed 5-character code; the buffer holds 5 chars + NUL terminator.
constexpr SQLSMALLINT SQL_STATE_BUFFER_SIZE = 6;
/// Buffer size for the connection string the driver echoes back from SQLDriverConnect.
constexpr SQLSMALLINT OUT_CONNECTION_STRING_SIZE = 1024;
/// Column-name buffer size used by SQLDescribeCol.
constexpr SQLSMALLINT COLUMN_NAME_BUFFER_SIZE = 256;
/// ColumnSize=26, DecimalDigits=6 → microsecond-precision timestamp
/// (YYYY-MM-DD HH:MM:SS.ffffff).
constexpr SQLULEN TIMESTAMP_COLUMN_SIZE = 26;
constexpr SQLSMALLINT TIMESTAMP_DECIMAL_DIGITS = 6;

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
        errorMessage << "  [" << sqlState.data() << "] (" << nativeError << ") " << message.data() << "\n";
        recNum++;
    }

    return errorMessage.str();
}

}

/// A constexpr template can't replicate this: `throw CannotOpenSource(...)` needs the caller's
/// source location for the exception's stack trace; the macro form preserves __FILE__:__LINE__.
/// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define CHECK(rc, handleType, handle, context) \
    do \
    { \
        const SQLRETURN check_rc = (rc); \
        if (check_rc != SQL_SUCCESS && check_rc != SQL_SUCCESS_WITH_INFO) \
        { \
            if (check_rc == SQL_ERROR) \
            { \
                throw CannotOpenSource("{}: {}", (context), showError((handleType), (handle), (context))); \
            } \
            NES_WARNING("{}:{}", (context), showError((handleType), (handle), (context))); \
        } \
    } while (0)

struct SqlStmtDeleter
{
    void operator()(SQLHSTMT handle) const noexcept
    {
        if (handle != SQL_NULL_HSTMT)
        {
            NES_INFO("Freeing STMT");
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
            NES_INFO("Delete Connection");
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
            NES_INFO("Delete Environment");
            SQLFreeHandle(SQL_HANDLE_ENV, handle);
        }
    }
};

using EnvironmentHandle = std::unique_ptr<void, SqlEnvironmentDeleter>;

struct Connection
{
    EnvironmentHandle env;
    ConnectionHandle connection;

    explicit Connection(const ConnectionInformation& connectionInformation)
    {
        /// `SERVER=<host>;PORT=<port>` — NOT `SERVER=<host>,<port>`. psqlodbc/libpq read a comma in
        /// SERVER as a multi-host list and ignore the PORT, so `SERVER=127.0.0.1,1` becomes hosts
        /// ["127.0.0.1", "1"] at the default port 5432 — and "1" parses as 0.0.0.1, whose connect
        /// hangs where the network drops the SYN (e.g. CI) because there is no login timeout. The
        /// semicolon form binds the real host:port and fails fast on a refused/closed port.
        auto connectionString = fmt::format(
            "DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};PWD={};TrustServerCertificate=yes;Encrypt=yes;CharSet=UTF-8",
            connectionInformation.driver,
            connectionInformation.server,
            connectionInformation.port,
            connectionInformation.database,
            connectionInformation.username,
            connectionInformation.password);

        NES_DEBUG("Connecting to database: {}", connectionString);
        {
            SQLHANDLE tmp = SQL_NULL_HENV;
            const auto rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &tmp);
            CHECK(rc, SQL_HANDLE_ENV, tmp, "SQLAllocHandle ENV");
            this->env = EnvironmentHandle{tmp};
        }

        SQLSetEnvAttr(env.get(), SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);

        {
            SQLHANDLE tmp = SQL_NULL_HDBC;
            const auto rc = SQLAllocHandle(SQL_HANDLE_DBC, env.get(), &tmp);
            CHECK(rc, SQL_HANDLE_DBC, tmp, "SQLAllocHandle DBC");
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
            SQL_DRIVER_COMPLETE);
        CHECK(rc, SQL_HANDLE_DBC, connection.get(), "SQLDriverConnect");
    }
};

namespace
{

void nullByteFixup(std::span<const std::byte> fixupMemory, std::span<std::byte> outputMemory)
{
    const auto* indicator = reinterpret_cast<const SQLBIGINT*>(fixupMemory.data());
    if (*indicator == SQL_NULL_DATA)
    {
        outputMemory[0] = std::byte(1);
    }
    else
    {
        INVARIANT(*indicator >= 0, "Negative Indicator caused by unhandled error: {}", *indicator);
    }
}

}

struct PreparedStatement
{
    struct ApplyFixup
    {
        std::span<const std::byte> input;
        std::span<std::byte> output;
        FixupFunction function;

        void operator()() const { function(input, output); }
    };

    struct ApplyVarSized
    {
        ColumnIndex columnIndex;
        CType cType;
        std::byte* nullable;
        std::span<std::byte> output;
    };

    StmtHandle statement;
    std::unique_ptr<SQL_TIMESTAMP_STRUCT> timestampLower;
    std::unique_ptr<SQL_TIMESTAMP_STRUCT> timestampUpper;
    std::vector<std::byte> outputTuple;
    size_t outputTupleSize{0};
    std::vector<std::byte> fixupTuple;
    std::vector<TupleBuffer> varsizedData;

    std::vector<ApplyFixup> fixupFunctions;
    std::vector<ApplyVarSized> varsizedAccesses;

    static std::vector<SQLType> getSqlResultTypes(SQLHSTMT hStmt)
    {
        SQLSMALLINT numCols = 0;
        const auto ret = SQLNumResultCols(hStmt, &numCols); /// how many columns the SELECT produces
        CHECK(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
        std::vector<SQLType> sqlTypes;
        sqlTypes.reserve(numCols);

        for (SQLSMALLINT i = 1; i <= numCols; ++i)
        {
            std::array<SQLCHAR, COLUMN_NAME_BUFFER_SIZE> name{};
            SQLSMALLINT nameLen = 0;
            SQLSMALLINT sqlType = 0;
            SQLULEN colSize = 0;
            SQLSMALLINT decimals = 0;
            SQLSMALLINT nullable = 0;

            const auto rc = SQLDescribeCol(
                hStmt,
                i,
                name.data(),
                static_cast<SQLSMALLINT>(name.size()),
                &nameLen,
                &sqlType, /// SQL_INTEGER, SQL_DOUBLE, SQL_VARCHAR, ...
                &colSize, /// for VARCHAR: max chars; for numerics: precision
                &decimals,
                &nullable); /// SQL_NULLABLE / SQL_NO_NULLS / SQL_NULLABLE_UNKNOWN
            CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLDescribeCol");
            sqlTypes.emplace_back(SQLType{.sqlType = sqlType, .isNullable = (nullable == SQL_NULLABLE)});
        }
        return sqlTypes;
    }

    static std::vector<SQLType> getSqlParameterTypes(SQLHSTMT hStmt)
    {
        SQLSMALLINT numCols = 0;
        const auto ret = SQLNumParams(hStmt, &numCols); /// how many ? statements are in the query
        CHECK(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
        std::vector<SQLType> sqlTypes;
        sqlTypes.reserve(numCols);

        for (SQLSMALLINT i = 1; i <= numCols; ++i)
        {
            SQLSMALLINT dataType = 0;
            SQLULEN paramSize = 0;
            SQLSMALLINT decimalDigits = 0;
            SQLSMALLINT nullable = 0;

            const auto rc = SQLDescribeParam(hStmt, i, &dataType, &paramSize, &decimalDigits, &nullable);

            CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLDescribeParam");
            sqlTypes.emplace_back(SQLType{.sqlType = dataType, .isNullable = (nullable == SQL_NULLABLE)});
        }
        return sqlTypes;
    }

    /// NOLINTNEXTLINE(readability-make-member-function-const): writes through the unique_ptr members (the pointed-to timestamps the bound parameters reference); declaring const here would misrepresent the semantics.
    void updateTimestampParam()
    {
        *timestampLower = *timestampUpper;
        *timestampUpper = converToTimestamp(std::chrono::system_clock::now());
    }

    static PreparedStatement bind(const Connection& connection, const Schema& schema, std::string query)
    {
        SQLHSTMT hStmt = SQL_NULL_HSTMT;
        auto rc = SQLAllocHandle(SQL_HANDLE_STMT, connection.connection.get(), &hStmt);
        CHECK(rc, SQL_HANDLE_DBC, connection.connection.get(), "SQLAllocHandle STMT");
        rc = SQLPrepare(hStmt, reinterpret_cast<SQLCHAR*>(query.data()), SQL_NTS);
        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLPrepare");
        auto statement = StmtHandle{hStmt};

        auto sqlTypes = getSqlResultTypes(hStmt);
        NES_DEBUG("Binding {} to {}.", fmt::join(sqlTypes, ", "), schema);

        if (sqlTypes.size() != schema.getNumberOfFields())
        {
            throw CannotOpenSource("Query returned {} columns, but schema expects {}.", sqlTypes.size(), schema.getNumberOfFields());
        }

        auto columnMappingResults = NES::views::enumerate(std::views::zip(sqlTypes, schema))
            | std::views::transform(
                                        [](const auto& types)
                                        {
                                            auto [index, type] = types;
                                            auto [sqlType, nesType] = type;
                                            return lookupColumnMapping(ColumnIndex(index), nesType.dataType, sqlType);
                                        })
            | std::ranges::to<std::vector<std::expected<ColumnMapping, std::string>>>();

        auto errors = columnMappingResults | std::views::filter([](const auto& result) { return !result.has_value(); })
            | std::views::transform([](const auto& result) -> std::string { return result.error(); })
            | std::ranges::to<std::vector<std::string>>();
        if (!errors.empty())
        {
            throw CannotOpenSource("Could not bind query to schema: \n{}", fmt::join(errors, "\n\t-"));
        }

        auto columnMappings = columnMappingResults | std::views::filter([](const auto& result) { return result.has_value(); })
            | std::views::transform([](const auto& result) { return result.value(); }) | std::ranges::to<std::vector<ColumnMapping>>();
        std::ranges::sort(columnMappings, {}, &ColumnMapping::columnIndex);

        size_t fixupSize = 0;
        const size_t tupleSize = schema.getSizeOfSchemaInBytes();
        for (auto& columnMapping : columnMappings)
        {
            if (columnMapping.nesType.nullable)
            {
                fixupSize += sizeof(SQLBIGINT);
            }
            fixupSize += std::visit(
                Overloaded{
                    [&](const Fixup& fixup) { return fixup.fixupSize; },
                    [&](const auto&) { return static_cast<size_t>(0); },
                },
                columnMapping.translation);
        }

        std::vector<std::byte> outputTuple(tupleSize);
        auto outputTupleMemory = std::span{outputTuple};
        std::vector<std::byte> fixupTuple(fixupSize);
        auto fixupTupleMemory = std::span{fixupTuple};

        std::vector<ApplyFixup> applyFixups;
        std::vector<ApplyVarSized> applyVarSized;
        for (const auto& columnMapping : columnMappings)
        {
            SQLBIGINT* nullValueIndicator = nullptr;
            std::byte* nesNullByte = nullptr;
            if (columnMapping.nesType.nullable)
            {
                if (!std::holds_alternative<VarSized>(columnMapping.translation))
                {
                    applyFixups.emplace_back(fixupTupleMemory.first(sizeof(SQLBIGINT)), outputTupleMemory.first(1), nullByteFixup);
                    nullValueIndicator = reinterpret_cast<SQLBIGINT*>(fixupTupleMemory.data());
                    fixupTupleMemory = fixupTupleMemory.subspan(sizeof(SQLBIGINT));
                }
                nesNullByte = outputTupleMemory.data();
                outputTupleMemory = outputTupleMemory.subspan(1);
            }

            std::visit(
                Overloaded{
                    [&](const Fixup& fixup)
                    {
                        applyFixups.emplace_back(
                            fixupTupleMemory.first(fixup.fixupSize),
                            outputTupleMemory.first(columnMapping.nesType.getSizeInBytesWithoutNull()),
                            fixup.fixupFunction);
                        rc = SQLBindCol(
                            hStmt,
                            columnMapping.columnIndex.index,
                            fixup.cType.type,
                            fixupTupleMemory.data(),
                            static_cast<SQLLEN>(fixup.fixupSize),
                            nullValueIndicator);
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                        fixupTupleMemory = fixupTupleMemory.subspan(fixup.fixupSize);
                    },
                    [&](const Direct& direct)
                    {
                        rc = SQLBindCol(
                            hStmt,
                            columnMapping.columnIndex.index,
                            direct.cType.type,
                            outputTupleMemory.data(),
                            columnMapping.nesType.getSizeInBytesWithoutNull(),
                            nullValueIndicator);
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                    },
                    [&](const VarSized& varSized)
                    {
                        applyVarSized.emplace_back(
                            columnMapping.columnIndex, varSized.cType, nesNullByte, outputTupleMemory.first(sizeof(VariableSizedAccess)));
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                    },
                },
                columnMapping.translation);

            outputTupleMemory = outputTupleMemory.subspan(columnMapping.nesType.getSizeInBytesWithoutNull());
        }


        auto parameterTypes = getSqlParameterTypes(hStmt);
        NES_DEBUG("Binding {} to {}.", fmt::join(parameterTypes, ", "), schema);
        auto timestampLower = std::make_unique<SQL_TIMESTAMP_STRUCT>(converToTimestamp(std::chrono::system_clock::now()));
        auto timestampUpper = std::make_unique<SQL_TIMESTAMP_STRUCT>(converToTimestamp(std::chrono::system_clock::now()));

        /// The previous values of (16, 0) meant minute precision; that matched the original MSSQL
        /// schema but truncated the bound parameter when used against postgres' microsecond-
        /// precision `timestamp` columns, so the windowed query returned no rows even when
        /// matching data existed.
        rc = SQLBindParameter(
            hStmt,
            1,
            SQL_PARAM_INPUT,
            SQL_C_TYPE_TIMESTAMP,
            SQL_TYPE_TIMESTAMP,
            TIMESTAMP_COLUMN_SIZE,
            TIMESTAMP_DECIMAL_DIGITS,
            timestampLower.get(),
            sizeof(SQL_TIMESTAMP_STRUCT),
            nullptr);
        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindParameter");

        rc = SQLBindParameter(
            hStmt,
            2,
            SQL_PARAM_INPUT,
            SQL_C_TYPE_TIMESTAMP,
            SQL_TYPE_TIMESTAMP,
            TIMESTAMP_COLUMN_SIZE,
            TIMESTAMP_DECIMAL_DIGITS,
            timestampUpper.get(),
            sizeof(SQL_TIMESTAMP_STRUCT),
            nullptr);
        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindParameter");

        auto preparedStatement = PreparedStatement{
            .statement = std::move(statement),
            .timestampLower = std::move(timestampLower),
            .timestampUpper = std::move(timestampUpper),
            .outputTuple = std::move(outputTuple),
            .outputTupleSize = tupleSize,
            .fixupTuple = std::move(fixupTuple),
            .varsizedData = {},
            .fixupFunctions = applyFixups,
            .varsizedAccesses = applyVarSized};
        return preparedStatement;
    }

    void exec()
    {
        updateTimestampParam();
        NES_DEBUG(
            "Fetching results between: {}({}) and {}({})",
            getTimestamp(*timestampLower),
            toNESTimestamp(getTimestamp(*timestampLower)),
            getTimestamp(*timestampUpper),
            toNESTimestamp(getTimestamp(*timestampUpper)));
        const SQLRETURN rc = SQLExecute(statement.get());
        CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLExecute");
    }

    bool fetch(TupleBuffer& parent, AbstractBufferProvider& provider)
    {
        const auto rc = SQLFetch(statement.get());
        if (rc == SQL_NO_DATA)
        {
            SQLFreeStmt(statement.get(), SQL_CLOSE);
            return false;
        }
        CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLFetch");

        for (const auto& [index, cType, nullable, destination] : varsizedAccesses)
        {
            std::array<SQLCHAR, 1> probe{};
            SQLLEN totalLen = 0;
            SQLRETURN getDataRc
                = SQLGetData(statement.get(), index.index, cType.type, probe.data(), static_cast<SQLLEN>(probe.size()), &totalLen);
            CHECK(getDataRc, SQL_HANDLE_STMT, statement.get(), "SQLGetData");


            if (totalLen == SQL_NULL_DATA)
            {
                INVARIANT(nullable, "SQLGetData returned SQL_NULL_DATA for a non nullable column.");
                *nullable = static_cast<std::byte>(1);
                continue;
            }

            if (totalLen == 0)
            {
                /// Empty variable-sized value: a zero Size makes the index a
                /// don't-care (no child buffer is read), so index 0 stands in
                /// for the branch's old INVALID_VARIABLE_SIZED_INDEX sentinel,
                /// which our tree's VariableSizedAccess no longer defines.
                *reinterpret_cast<VariableSizedAccess*>(destination.data())
                    = VariableSizedAccess{VariableSizedAccess::Index{0}, VariableSizedAccess::Size{0}};
                continue;
            }

            TupleBuffer resultBuffer;
            if (totalLen == SQL_NO_TOTAL)
            {
                /// SQL Server cannot tell the size of the variable sized data, and i dont have the patience to implement that properly. We will allocate what ever fixed size buffer and attempt to fill or truncate the variable sized data.
                resultBuffer = provider.getBufferBlocking();
                totalLen = static_cast<SQLLEN>(resultBuffer.getBufferSize());
            }
            else
            {
                INVARIANT(totalLen >= 0, "SQLGetData returned a negative length: {}.", totalLen);
                /// Account for zero termination
                resultBuffer = provider.getUnpooledBuffer(totalLen + 1).value();
            }

            SQLLEN bytesRead = 0;
            getDataRc = SQLGetData(
                statement.get(),
                index.index,
                cType.type,
                resultBuffer.getAvailableMemoryArea<std::byte>().data(),
                totalLen + 1,
                &bytesRead);
            CHECK(getDataRc, SQL_HANDLE_STMT, statement.get(), "SQLGetData");
            auto childBufferIndex = parent.storeChildBuffer(resultBuffer);
            NES_DEBUG("SQLGetData returned {} bytes.", bytesRead);
            *reinterpret_cast<VariableSizedAccess*>(destination.data())
                = VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{static_cast<uint64_t>(bytesRead)}};
        }

        for (const auto& fixup : fixupFunctions)
        {
            fixup();
        }

        return true;
    }

    [[nodiscard]] std::span<const std::byte> data() const { return {outputTuple.data(), outputTupleSize}; }
};

struct ConnectingToDatabaseState

{
};

struct FetchFromDatabaseState
{
    Connection connection;
    PreparedStatement statement;
};

struct FetchFromBufferState
{
    Connection connection;
    PreparedStatement statement;
    Timestamp watermark;
};

using SourceState = std::variant<ConnectingToDatabaseState, FetchFromDatabaseState, FetchFromBufferState>;

struct Context
{
    ConnectionInformation connectionInformation;
    SourceState state;
};

ODBCSource::ODBCSource(const SourceDescriptor& sourceDescriptor)
    : pollIntervalMs(sourceDescriptor.getFromConfig(ConfigParametersODBC::POLL_INTERVAL_MS))
    , query(sourceDescriptor.getFromConfig(ConfigParametersODBC::QUERY))
    , schema(*sourceDescriptor.getLogicalSource().getSchema())
    , sourceContext(std::make_unique<Context>(
          ConnectionInformation{
              .server = sourceDescriptor.getFromConfig(ConfigParametersODBC::SERVER),
              .port = sourceDescriptor.getFromConfig(ConfigParametersODBC::PORT),
              .database = sourceDescriptor.getFromConfig(ConfigParametersODBC::DATABASE),
              .username = sourceDescriptor.getFromConfig(ConfigParametersODBC::USERNAME),
              .password = sourceDescriptor.getFromConfig(ConfigParametersODBC::PASSWORD),
              .driver = sourceDescriptor.getFromConfig(ConfigParametersODBC::DRIVER)},
          ConnectingToDatabaseState{}))
{
}

std::ostream& ODBCSource::toString(std::ostream& str) const
{
    str << "\nODBCSource(";
    str << "\n  generated tuples: " << this->generatedTuples;
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << ")\n";
    return str;
}

ODBCSource::~ODBCSource() = default;

void ODBCSource::open(std::shared_ptr<AbstractBufferProvider> bufferProviderPtr)
{
    this->bufferProvider = std::move(bufferProviderPtr);
    /// Connect + bind eagerly here so an unreachable endpoint or an
    /// unbindable schema fails fast in open() rather than mid-ingest: the
    /// conformance battery's bad_endpoint scenario expects an open-phase
    /// connector error, and the harness's READY/GO handshake only sends GO
    /// *after* open() returns — a connect deferred to fillTupleBuffer would
    /// otherwise block waiting for a GO that never comes. The query EXEC
    /// stays deferred to fillTupleBuffer (FetchFromDatabaseState), so a
    /// round_trip seed committed after READY is still visible to the first
    /// poll's (watermark, now] window.
    Connection connection(sourceContext->connectionInformation);
    auto statement = PreparedStatement::bind(connection, schema, query);
    sourceContext->state = FetchFromDatabaseState{.connection = std::move(connection), .statement = std::move(statement)};
    NES_DEBUG("ODBCSource connected successfully.");
}

namespace
{
bool waitForTimeout(std::chrono::milliseconds timeout, const std::stop_token& stoken)
{
    std::mutex mtx;
    std::condition_variable_any cv;
    std::unique_lock lock(mtx);
    cv.wait_for(lock, stoken, timeout, [&stoken] { return stoken.stop_requested(); });
    return !stoken.stop_requested();
}
}

Source::FillTupleBufferResult ODBCSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stoken)
{
    auto currentBuffer = tupleBuffer.getAvailableMemoryArea<std::byte>();
    size_t numberOfTuples = 0;
    bool shouldReturn = false;
    auto watermark = Timestamp(0);

    while (!shouldReturn && !stoken.stop_requested())
    {
        sourceContext->state = std::visit(
            Overloaded{
                /// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): the variant's content is replaced via the returned state value; the visitor binds the source rvalue but does not move from the empty ConnectingToDatabaseState.
                [this](ConnectingToDatabaseState&&) -> SourceState
                {
                    Connection connection(sourceContext->connectionInformation);
                    auto statement = PreparedStatement::bind(connection, schema, query);
                    return FetchFromDatabaseState{.connection = std::move(connection), .statement = std::move(statement)};
                },
                /// NOLINTNEXTLINE(cppcoreguidelines-rvalue-reference-param-not-moved): moved-from at the matching return sites (`FetchFromBufferState{.connection = std::move(state.connection), …}`).
                [&](FetchFromDatabaseState&& state) -> SourceState
                {
                    if (!waitForTimeout(std::chrono::milliseconds(pollIntervalMs), stoken))
                    {
                        NES_DEBUG("ODBCSource was requested to stop");
                        return state;
                    }

                    state.statement.exec();
                    const auto timestamp = *state.statement.timestampLower;
                    return FetchFromBufferState{
                        .connection = std::move(state.connection),
                        .statement = std::move(state.statement),
                        .watermark = Timestamp(toNESTimestamp(getTimestamp(timestamp)))};
                },
                [&](FetchFromBufferState&& state) -> SourceState
                {
                    if (currentBuffer.size() < state.statement.data().size())
                    {
                        watermark = state.watermark;
                        shouldReturn = true;
                        return FetchFromBufferState{
                            .connection = std::move(state.connection),
                            .statement = std::move(state.statement),
                            .watermark = std::move(watermark)};
                    }

                    const auto hasTuple = state.statement.fetch(tupleBuffer, *bufferProvider);
                    if (!hasTuple)
                    {
                        watermark = Timestamp(toNESTimestamp(getTimestamp(*state.statement.timestampUpper)));
                        shouldReturn = true;
                        return FetchFromDatabaseState{.connection = std::move(state.connection), .statement = std::move(state.statement)};
                    }

                    std::ranges::copy(state.statement.data(), currentBuffer.begin());
                    currentBuffer = currentBuffer.subspan(state.statement.data().size());
                    numberOfTuples += 1;
                    return FetchFromBufferState{std::move(state)};
                }},
            std::move(sourceContext->state));
    }

    NES_DEBUG("Emtitting Buffer with watermark: {}", watermark);
    tupleBuffer.setWatermark(watermark);

    return FillTupleBufferResult::withBytes(numberOfTuples);
}

DescriptorConfig::Config ODBCSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// Probe DRIVER's presence eagerly so a missing key throws std::out_of_range here
    /// (the validateAndFormat path treats DRIVER as required for the connection string).
    (void)config.at(ConfigParametersODBC::DRIVER);
    return DescriptorConfig::validateAndFormat<ConfigParametersODBC>(std::move(config), NAME);
}

void ODBCSource::close()
{
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): the registry's generated *.inc declares this entry point by value; switching to const& breaks linkage.
SourceValidationRegistryReturnType RegisterODBCSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ODBCSource::validateAndFormat(sourceConfig.config);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): the registry's generated *.inc declares this entry point by value; switching to const& breaks linkage.
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterODBCSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<ODBCSource>(sourceRegistryArguments.sourceDescriptor);
}
}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length)
