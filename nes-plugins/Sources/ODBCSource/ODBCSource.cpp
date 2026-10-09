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
#include <charconv>
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
#include <stop_token>
#include <string>
#include <system_error>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Interface/VariableSizedAccess.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Ranges.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <ODBCConnection.hpp>
#include <ODBCTypeMappings.hpp>

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

using ODBC::Connection;
using ODBC::ConnectionInformation;
using ODBC::StmtHandle;

namespace
{

/// Column-name buffer size used by SQLDescribeCol.
constexpr SQLSMALLINT COLUMN_NAME_BUFFER_SIZE = 256;
/// ColumnSize=26, DecimalDigits=6 → microsecond-precision timestamp
/// (YYYY-MM-DD HH:MM:SS.ffffff).
constexpr SQLULEN TIMESTAMP_COLUMN_SIZE = 26;
constexpr SQLSMALLINT TIMESTAMP_DECIMAL_DIGITS = 6;
/// The incremental poller binds exactly one `?` parameter: the lower watermark bound. Each poll
/// runs `WHERE <ts> > ?` (no wall-clock upper bound) and advances the watermark to the max
/// timestamp actually read, so no row is dropped to commit latency or writer/NES clock skew — the
/// old (watermark, now] window lost every row whose timestamp landed at/after NES's poll instant.
constexpr std::size_t WINDOW_PARAMETER_COUNT = 1;

/// The source has always used the driver's default login timeout; the shared Connection takes it
/// as an explicit argument so a caller cannot forget that a timeout exists to be set.
constexpr SQLUINTEGER NO_LOGIN_TIMEOUT = 0;

/// Chronological comparison of two SQL_TIMESTAMP_STRUCTs (field-lexicographic == time order).
inline bool isAfter(const SQL_TIMESTAMP_STRUCT& a, const SQL_TIMESTAMP_STRUCT& b)
{
    return std::tie(a.year, a.month, a.day, a.hour, a.minute, a.second, a.fraction)
        > std::tie(b.year, b.month, b.day, b.hour, b.minute, b.second, b.fraction);
}

}

namespace
{

/// Translates ODBC's length/indicator value into the NES per-field null byte. Bound as a
/// fixup so it runs on every fetch alongside the value conversions.
void nullByteFixup(std::span<const std::byte> fixupMemory, std::span<std::byte> outputMemory)
{
    SQLBIGINT indicator = 0;
    std::memcpy(&indicator, fixupMemory.data(), sizeof(indicator));
    if (indicator == SQL_NULL_DATA)
    {
        outputMemory[0] = std::byte(1);
    }
    else
    {
        INVARIANT(indicator >= 0, "Negative Indicator caused by unhandled error: {}", indicator);
        /// The output tuple is reused across fetches; clear the flag so a non-null
        /// value after a NULL row is not left marked NULL by the prior fetch.
        outputMemory[0] = std::byte(0);
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
        /// Points at the field's null byte in the output tuple, or nullptr when the column is
        /// not nullable. Variable-sized columns learn their nullness from SQLGetData rather
        /// than a bound indicator, so the flag is written during fetch().
        std::byte* nullable;
        std::span<std::byte> output;
    };

    StmtHandle statement;
    std::unique_ptr<SQL_TIMESTAMP_STRUCT> timestampLower;
    /// Byte offset, within fixupTuple, of the watermark column's raw SQL_TIMESTAMP_STRUCT. Set at
    /// bind time when the query has a TIMESTAMP column. After each fetched row the watermark
    /// (timestampLower, the single `?` lower bound) is advanced to the max timestamp read, so the
    /// next poll reads `WHERE <ts> > <max-seen>`.
    size_t watermarkFixupOffset{0};
    bool hasWatermarkColumn{false};
    /// Whole-hour shift applied to the UTC wall clock for the initial watermark, so it matches a
    /// source column stored in a non-UTC timezone (see ConfigParametersODBC).
    std::chrono::hours timezoneOffset{0};
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
        NES_ODBC_CHECK(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
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
            NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLDescribeCol");
            sqlTypes.emplace_back(SQLType{.sqlType = sqlType, .isNullable = (nullable == SQL_NULLABLE)});
        }
        return sqlTypes;
    }

    static std::vector<SQLType> getSqlParameterTypes(SQLHSTMT hStmt)
    {
        SQLSMALLINT numCols = 0;
        const auto ret = SQLNumParams(hStmt, &numCols); /// how many ? statements are in the query
        NES_ODBC_CHECK(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
        std::vector<SQLType> sqlTypes;
        sqlTypes.reserve(numCols);

        for (SQLSMALLINT i = 1; i <= numCols; ++i)
        {
            SQLSMALLINT dataType = 0;
            SQLULEN paramSize = 0;
            SQLSMALLINT decimalDigits = 0;
            SQLSMALLINT nullable = 0;

            const auto rc = SQLDescribeParam(hStmt, i, &dataType, &paramSize, &decimalDigits, &nullable);

            NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLDescribeParam");
            sqlTypes.emplace_back(SQLType{.sqlType = dataType, .isNullable = (nullable == SQL_NULLABLE)});
        }
        return sqlTypes;
    }

    /// Advance the lower watermark to the last fetched row's timestamp if it is newer, so the next
    /// poll reads strictly after the max timestamp seen so far. Reads the raw SQL_TIMESTAMP the
    /// driver wrote into fixupTuple for the watermark column during SQLFetch.
    /// NOLINTNEXTLINE(readability-make-member-function-const): mutates the pointed-to bound parameter.
    void advanceWatermarkFromLastRow()
    {
        if (!hasWatermarkColumn)
        {
            return;
        }
        SQL_TIMESTAMP_STRUCT rowTs{};
        std::memcpy(&rowTs, fixupTuple.data() + watermarkFixupOffset, sizeof(SQL_TIMESTAMP_STRUCT));
        if (isAfter(rowTs, *timestampLower))
        {
            *timestampLower = rowTs;
        }
    }

    static PreparedStatement bind(
        const Connection& connection,
        const Schema<UnqualifiedUnboundField, Ordered>& schema,
        std::string query,
        std::chrono::hours timezoneOffset)
    {
        SQLHSTMT hStmt = SQL_NULL_HSTMT;
        auto rc = SQLAllocHandle(SQL_HANDLE_STMT, connection.connection.get(), &hStmt);
        NES_ODBC_CHECK(rc, SQL_HANDLE_DBC, connection.connection.get(), "SQLAllocHandle STMT");
        rc = SQLPrepare(hStmt, reinterpret_cast<SQLCHAR*>(query.data()), SQL_NTS);
        NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLPrepare");
        auto statement = StmtHandle{hStmt};

        auto sqlTypes = getSqlResultTypes(hStmt);
        NES_DEBUG("Binding {} to {}.", fmt::join(sqlTypes, ", "), schema);

        if (sqlTypes.size() != schema.size())
        {
            throw CannotOpenSource("Query returned {} columns, but schema expects {}.", sqlTypes.size(), schema.size());
        }

        auto columnMappingResults = NES::views::enumerate(std::views::zip(sqlTypes, schema))
            | std::views::transform(
                                        [](const auto& types)
                                        {
                                            auto [index, type] = types;
                                            auto [sqlType, nesType] = type;
                                            return lookupColumnMapping(ColumnIndex(index), nesType.getDataType(), sqlType);
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
        const size_t tupleSize = getSizeInBytes(schema);
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
        size_t watermarkFixupOffset = 0;
        bool watermarkColumnFound = false;
        for (const auto& columnMapping : columnMappings)
        {
            /// A nullable column carries a leading null byte in the NES tuple. Fixed-width and
            /// fixup columns learn their nullness from the length/indicator ODBC writes during
            /// SQLFetch, so reserve room for it in the fixup tuple and translate it via
            /// nullByteFixup. Variable-sized columns instead get a pointer to the null byte and
            /// set it from SQLGetData's indicator in fetch().
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
                        NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                        /// The (single) TIMESTAMP column is the watermark column: record where the
                        /// driver writes its raw SQL_TIMESTAMP_STRUCT so the poller can read the row's
                        /// timestamp back and advance the `> ?` lower bound (advanceWatermarkFromLastRow).
                        if (fixup.cType.type == SQL_C_TYPE_TIMESTAMP && !watermarkColumnFound)
                        {
                            watermarkFixupOffset = static_cast<size_t>(fixupTupleMemory.data() - std::span{fixupTuple}.data());
                            watermarkColumnFound = true;
                        }
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
                        NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                    },
                    [&](const VarSized& varSized)
                    {
                        applyVarSized.emplace_back(
                            columnMapping.columnIndex, varSized.cType, nesNullByte, outputTupleMemory.first(sizeof(VariableSizedAccess)));
                        NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                    },
                },
                columnMapping.translation);

            outputTupleMemory = outputTupleMemory.subspan(columnMapping.nesType.getSizeInBytesWithoutNull());
        }


        auto parameterTypes = getSqlParameterTypes(hStmt);
        NES_DEBUG("Binding {} to {}.", fmt::join(parameterTypes, ", "), schema);
        INVARIANT(
            parameterTypes.size() == WINDOW_PARAMETER_COUNT,
            "validateAndFormat enforces exactly {} '?' parameter, but the prepared query reports {}.",
            WINDOW_PARAMETER_COUNT,
            parameterTypes.size());
        /// Single lower bound: start the watermark at "now" so the poller streams rows inserted from
        /// this point on (set to epoch 0 instead to backfill all history). It advances to the max row
        /// timestamp read (advanceWatermarkFromLastRow); there is no upper bound.
        auto timestampLower = std::make_unique<SQL_TIMESTAMP_STRUCT>(convertToTimestamp(std::chrono::system_clock::now() + timezoneOffset));

        /// ColumnSize/DecimalDigits give microsecond precision (YYYY-MM-DD HH:MM:SS.ffffff); the
        /// earlier (16, 0) meant minute precision and truncated the bound parameter against
        /// microsecond-precision `timestamp` columns, so the query returned no rows.
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
        NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindParameter");

        auto preparedStatement = PreparedStatement{
            .statement = std::move(statement),
            .timestampLower = std::move(timestampLower),
            .watermarkFixupOffset = watermarkFixupOffset,
            .hasWatermarkColumn = watermarkColumnFound,
            .timezoneOffset = timezoneOffset,
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
        NES_DEBUG(
            "ODBCSource poll: fetching rows with timestamp > {} (NES {})",
            getTimestamp(*timestampLower),
            toNESTimestamp(getTimestamp(*timestampLower)));
        const SQLRETURN rc = SQLExecute(statement.get());
        NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLExecute");
    }

    bool fetch(TupleBuffer& parent, AbstractBufferProvider& provider)
    {
        const auto rc = SQLFetch(statement.get());
        if (rc == SQL_NO_DATA)
        {
            NES_DEBUG("ODBCSource fetch: SQL_NO_DATA (end of this poll's result set)");
            SQLFreeStmt(statement.get(), SQL_CLOSE);
            return false;
        }
        NES_ODBC_CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLFetch");

        NES_DEBUG("ODBCSource fetch: got a row");
        for (const auto& [index, cType, nullable, destination] : varsizedAccesses)
        {
            /// The output tuple is reused across fetches; clear the flag up front so
            /// a non-null value after a NULL row is not left marked NULL (only the
            /// SQL_NULL_DATA branch below sets it).
            if (nullable != nullptr)
            {
                *nullable = static_cast<std::byte>(0);
            }
            std::array<SQLCHAR, 1> probe{};
            SQLLEN totalLen = 0;
            SQLRETURN getDataRc
                = SQLGetData(statement.get(), index.index, cType.type, probe.data(), static_cast<SQLLEN>(probe.size()), &totalLen);
            NES_ODBC_CHECK(getDataRc, SQL_HANDLE_STMT, statement.get(), "SQLGetData");


            if (totalLen == SQL_NULL_DATA)
            {
                INVARIANT(nullable, "SQLGetData returned SQL_NULL_DATA for a non nullable column.");
                *nullable = static_cast<std::byte>(1);
                /// Zero the slot too (size 0): the output tuple is reused across
                /// fetches, so a stale non-zero size from a prior row would make a
                /// reader emit a bogus child payload for this NULL value.
                const auto emptyAccess = VariableSizedAccess{ChildBufferIndex{0}, VariableSizedAccess::Size{0}};
                std::memcpy(destination.data(), &emptyAccess, sizeof(emptyAccess));
                continue;
            }

            if (totalLen == 0)
            {
                /// Empty variable-sized value: a zero Size makes the index a don't-care
                /// (no child buffer is read), so index 0 stands in for the old
                /// INVALID_VARIABLE_SIZED_INDEX sentinel, which our tree's
                /// VariableSizedAccess no longer defines.
                const auto emptyAccess = VariableSizedAccess{ChildBufferIndex{0}, VariableSizedAccess::Size{0}};
                std::memcpy(destination.data(), &emptyAccess, sizeof(emptyAccess));
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


            NES_ODBC_CHECK(getDataRc, SQL_HANDLE_STMT, statement.get(), "SQLGetData");
            auto childBufferIndex = parent.storeChildBuffer(resultBuffer);

            NES_DEBUG("SQLGetData returned {} bytes.", bytesRead);
            /// Do not read resultBuffer past this point: storeChildBuffer() takes it by
            /// non-const reference and empties it, so getAvailableMemoryArea() now yields a
            /// null pointer. A string_view built over it here aborts under libc++ hardening
            /// (`__len == 0 || __s != nullptr`) on the first non-empty varsized value.
            const auto access = VariableSizedAccess{childBufferIndex, VariableSizedAccess::Size{static_cast<uint64_t>(bytesRead)}};
            std::memcpy(destination.data(), &access, sizeof(access));
        }

        for (const auto& fixup : fixupFunctions)
        {
            fixup();
        }

        /// Advance the `> ?` lower bound to this row's timestamp (the max, since rows are read ASC),
        /// so the next poll continues strictly after it. Keyed to the data, not the wall clock, so
        /// no row is lost to commit latency or writer/NES clock skew.
        advanceWatermarkFromLastRow();
        if (hasWatermarkColumn)
        {
            SQL_TIMESTAMP_STRUCT rowTs{};
            std::memcpy(&rowTs, fixupTuple.data() + watermarkFixupOffset, sizeof(SQL_TIMESTAMP_STRUCT));
            NES_DEBUG("ODBCSource fetch: row timestamp {}, watermark now {}", getTimestamp(rowTs), getTimestamp(*timestampLower));
        }

        NES_DEBUG(
            "ODBCSource fetch: fetched tuple ({} bytes): {:02x}",
            outputTupleSize,
            fmt::join(
                std::span{outputTuple.data(), outputTupleSize}
                    | std::views::transform([](const std::byte byte) { return std::to_integer<unsigned>(byte); }),
                ""));

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
    , timezoneOffset(std::chrono::hours(sourceDescriptor.getFromConfig(ConfigParametersODBC::TIMEZONE_OFFSET_HOURS)))
    , sourceContext(std::make_unique<Context>(
          ConnectionInformation{
              .server = sourceDescriptor.getFromConfig(ConfigParametersODBC::DB_HOST),
              .port = sourceDescriptor.getFromConfig(ConfigParametersODBC::DB_PORT),
              .database = sourceDescriptor.getFromConfig(ConfigParametersODBC::DATABASE),
              .username = sourceDescriptor.getFromConfig(ConfigParametersODBC::USERNAME),
              .password = sourceDescriptor.getFromConfig(ConfigParametersODBC::PASSWORD),
              .driver = sourceDescriptor.getFromConfig(ConfigParametersODBC::DRIVER),
              .trustServerCertificate = sourceDescriptor.getFromConfig(ConfigParametersODBC::TRUST_SERVER_CERTIFICATE)},
          ConnectingToDatabaseState{}))
{
}

std::ostream& ODBCSource::toString(std::ostream& str) const
{
    str << fmt::format("\nODBCSource(\n  pollIntervalMs: {}\n  query: {}\n  schema: {}\n)\n", pollIntervalMs, query, schema);
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
    Connection connection(sourceContext->connectionInformation, NO_LOGIN_TIMEOUT);
    auto statement = PreparedStatement::bind(connection, schema, query, timezoneOffset);
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
                    Connection connection(sourceContext->connectionInformation, NO_LOGIN_TIMEOUT);
                    auto statement = PreparedStatement::bind(connection, schema, query, timezoneOffset);
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
                        watermark = Timestamp(toNESTimestamp(getTimestamp(*state.statement.timestampLower)));
                        shouldReturn = true;
                        return FetchFromDatabaseState{.connection = std::move(state.connection), .statement = std::move(state.statement)};
                    }

                    {
                        std::string hex;
                        for (const auto b : state.statement.data())
                        {
                            hex += fmt::format("{:02x}", std::to_integer<unsigned>(b));
                        }
                        NES_DEBUG(
                            "ODBCSource writing tuple #{} to buffer ({} bytes): {}", numberOfTuples, state.statement.data().size(), hex);
                    }
                    std::ranges::copy(state.statement.data(), currentBuffer.begin());
                    currentBuffer = currentBuffer.subspan(state.statement.data().size());
                    numberOfTuples += 1;
                    return FetchFromBufferState{std::move(state)};
                }},
            std::move(sourceContext->state));
    }

    NES_DEBUG("ODBCSource emitting buffer: {} tuple(s), watermark {}", numberOfTuples, watermark);
    tupleBuffer.setWatermark(watermark);

    /// A tuple count, despite the factory's name — and it has to be, because the number this
    /// source returns is exactly the number that reaches the buffer.
    ///
    /// SourceThread does `setNumberOfTuples(fillTupleResult.getNumberOfBytes())`
    /// unconditionally, overwriting anything the source set on the buffer itself, so the
    /// return value is the only lever a source has over the final tuple count. For a
    /// byte-oriented source that is correct: an input formatter later reinterprets the byte
    /// count and rewrites the field. This source is NATIVE — it writes engine tuples directly
    /// and PipeliningPhase inserts no formatter behind it — so nothing would ever convert a
    /// byte count, and the value handed over must already be the tuple count.
    return FillTupleBufferResult::withBytes(numberOfTuples);
}

DescriptorConfig::Config ODBCSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    /// Probe DRIVER's presence eagerly so a missing key throws std::out_of_range here
    /// (the validateAndFormat path treats DRIVER as required for the connection string).
    (void)config.at(ConfigParametersODBC::DRIVER);

    /// The incremental poller binds exactly one `?` parameter (the lower watermark bound),
    /// so reject any other count here rather than at connect time.
    if (const auto query = config.find(ConfigParametersODBC::QUERY); query != config.end())
    {
        if (const auto numParameters = std::ranges::count(query->second, '?'); std::cmp_not_equal(numParameters, WINDOW_PARAMETER_COUNT))
        {
            throw InvalidConfigParameter(
                "ODBC source query must have exactly {} '?' parameter for the `> watermark` lower bound, but has {}.",
                WINDOW_PARAMETER_COUNT,
                numParameters);
        }
    }

    /// The timezone offset only shifts the window's wall-clock bounds by whole hours; a value beyond a
    /// full day's rotation is a configuration mistake. Reject it here (a non-integer string is left to
    /// the type-checked validateAndFormat below to report).
    if (const auto it = config.find(ConfigParametersODBC::TIMEZONE_OFFSET_HOURS); it != config.end())
    {
        int32_t offset = 0;
        const auto* const end = it->second.data() + it->second.size();
        const auto [ptr, ec] = std::from_chars(it->second.data(), end, offset);
        if (ec == std::errc{} && ptr == end && (offset < -24 || offset > 24))
        {
            throw InvalidConfigParameter("ODBC source timezone_offset_hours must be within [-24, 24], but is {}.", offset);
        }
    }

    return DescriptorConfig::validateAndFormat<ConfigParametersODBC>(std::move(config), NAME);
}

void ODBCSource::close()
{
}
}

/// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast,readability-identifier-length)
