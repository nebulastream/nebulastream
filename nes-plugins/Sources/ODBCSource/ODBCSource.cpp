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

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <exception>
#include <memory>
#include <ostream>
#include <ranges>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <netdb.h>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <google/protobuf/source_context.pb.h>
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <ODBCTypeMappings.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

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

std::string showError(SQLSMALLINT handleType, SQLHANDLE handle, const std::string& context)
{
    SQLCHAR sqlState[6];
    SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
    SQLINTEGER nativeError;
    SQLSMALLINT messageLen;
    SQLSMALLINT recNum = 1;

    std::stringstream errorMessage;
    errorMessage << "Error in " << context << ":\n";
    while (SQLGetDiagRec(handleType, handle, recNum, sqlState, &nativeError, message, sizeof(message), &messageLen) == SQL_SUCCESS)
    {
        errorMessage << "  [" << sqlState << "] (" << nativeError << ") " << message << "\n";
        recNum++;
    }

    return errorMessage.str();
}

#define CHECK(rc, handleType, handle, context) \
    if ((rc) != SQL_SUCCESS && (rc) != SQL_SUCCESS_WITH_INFO) \
    { \
        if ((rc) == SQL_ERROR) \
            throw CannotOpenSource("{}: {}", context, showError((handleType), (handle), (context))); \
        else \
            NES_WARNING("{}:{}", context, showError((handleType), (handle), (context))); \
    }

struct SqlStmtDeleter
{
    void operator()(SQLHSTMT h) const noexcept
    {
        if (h != SQL_NULL_HSTMT)
        {
            NES_INFO("Freeing STMT");
            SQLFreeHandle(SQL_HANDLE_STMT, h);
        }
    }
};

using StmtHandle = std::unique_ptr<void, SqlStmtDeleter>;

struct SqlConnectionDeleter
{
    void operator()(SQLHDBC h) const noexcept
    {
        if (h != SQL_NULL_HDBC)
        {
            NES_INFO("Delete Connection");
            SQLDisconnect(h);
            SQLFreeHandle(SQL_HANDLE_DBC, h);
        }
    }
};

using ConnectionHandle = std::unique_ptr<void, SqlConnectionDeleter>;

struct SqlEnvironmentDeleter
{
    void operator()(SQLHENV h) const noexcept
    {
        if (h != SQL_NULL_HENV)
        {
            NES_INFO("Delete Environment");
            SQLFreeHandle(SQL_HANDLE_ENV, h);
        }
    }
};

using EnvironmentHandle = std::unique_ptr<void, SqlEnvironmentDeleter>;

struct Connection
{
    EnvironmentHandle env;
    ConnectionHandle connection;

    Connection(const ConnectionInformation& connectionInformation)
    {
        auto connectionString = fmt::format(
            "DRIVER={{{}}};SERVER={},{};DATABASE={};UID={};PWD={};TrustServerCertificate=yes;Encrypt=yes;CharSet=UTF-8",
            connectionInformation.driver,
            connectionInformation.server,
            connectionInformation.port,
            connectionInformation.database,
            connectionInformation.username,
            connectionInformation.password);

        NES_DEBUG("Connecting to database: {}", connectionString);
        {
            SQLHANDLE tmp = SQL_NULL_HENV;
            auto rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &tmp);
            CHECK(rc, SQL_HANDLE_ENV, tmp, "SQLAllocHandle ENV");
            this->env = EnvironmentHandle{tmp};
        }

        SQLSetEnvAttr(env.get(), SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);

        {
            SQLHANDLE tmp = SQL_NULL_HDBC;
            auto rc = SQLAllocHandle(SQL_HANDLE_DBC, env.get(), &tmp);
            CHECK(rc, SQL_HANDLE_DBC, tmp, "SQLAllocHandle DBC");
            this->connection = ConnectionHandle{tmp};
        }

        SQLCHAR outConnStr[1024];
        SQLSMALLINT outConnStrLen;
        const auto rc = SQLDriverConnect(
            connection.get(),
            NULL,
            reinterpret_cast<SQLCHAR*>(connectionString.data()),
            SQL_NTS,
            outConnStr,
            sizeof(outConnStr),
            &outConnStrLen,
            SQL_DRIVER_COMPLETE);
        CHECK(rc, SQL_HANDLE_DBC, connection.get(), "SQLDriverConnect");
    }
};

void nullByteFixup(std::span<const std::byte> fixupMemory, std::span<std::byte> outputMemory)
{
    auto* inidicator = reinterpret_cast<const SQLBIGINT*>(fixupMemory.data());
    if (*inidicator == SQL_NULL_DATA)
    {
        outputMemory[0] = std::byte(1);
    }
    else
    {
        INVARIANT(*inidicator >= 0, "Negative Indicator caused by unhandled error: {}", *inidicator);
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
    std::unique_ptr<std::byte[]> outputTuple;
    size_t outputTupleSize;
    std::unique_ptr<std::byte[]> fixupTuple;
    std::vector<TupleBuffer> varsizedData;

    std::vector<ApplyFixup> fixupFunctions;
    std::vector<ApplyVarSized> varsizedAccesses;

    static std::vector<SQLType> getSqlResultTypes(SQLHSTMT hStmt)
    {
        SQLSMALLINT numCols = 0;
        auto ret = SQLNumResultCols(hStmt, &numCols); // how many columns the SELECT produces
        CHECK(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
        std::vector<SQLType> sqlTypes;
        sqlTypes.reserve(numCols);

        for (SQLSMALLINT i = 1; i <= numCols; ++i)
        {
            SQLCHAR name[256];
            SQLSMALLINT nameLen = 0;
            SQLSMALLINT sqlType = 0;
            SQLULEN colSize = 0;
            SQLSMALLINT decimals = 0;
            SQLSMALLINT nullable = 0;

            auto rc = SQLDescribeCol(
                hStmt,
                i,
                name,
                sizeof(name),
                &nameLen,
                &sqlType, // SQL_INTEGER, SQL_DOUBLE, SQL_VARCHAR, ...
                &colSize, // for VARCHAR: max chars; for numerics: precision
                &decimals,
                &nullable); // SQL_NULLABLE / SQL_NO_NULLS / SQL_NULLABLE_UNKNOWN
            CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLDescribeCol");
            sqlTypes.emplace_back(SQLType{.sqlType = sqlType, .isNullable = (nullable == SQL_NULLABLE)});
        }
        return sqlTypes;
    }

    static std::vector<SQLType> getSqlParameterTypes(SQLHSTMT hStmt)
    {
        SQLSMALLINT numCols = 0;
        auto ret = SQLNumParams(hStmt, &numCols); // how many ? statements are in the query
        CHECK(ret, SQL_HANDLE_STMT, hStmt, "SQLNumResultCols");
        std::vector<SQLType> sqlTypes;
        sqlTypes.reserve(numCols);

        for (SQLSMALLINT i = 1; i <= numCols; ++i)
        {
            SQLSMALLINT dataType;
            SQLULEN paramSize;
            SQLSMALLINT decimalDigits;
            SQLSMALLINT nullable;

            auto rc = SQLDescribeParam(hStmt, i, &dataType, &paramSize, &decimalDigits, &nullable);

            CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLDescribeParam");
            sqlTypes.emplace_back(SQLType{.sqlType = dataType, .isNullable = (nullable == SQL_NULLABLE)});
        }
        return sqlTypes;
    }

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

        auto columnMappingResults = std::views::enumerate(std::views::zip(sqlTypes, schema))
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
        size_t tupleSize = schema.getSizeOfSchemaInBytes();
        for (auto& column_mapping : columnMappings)
        {
            if (column_mapping.nesType.nullable)
            {
                fixupSize += sizeof(SQLBIGINT);
            }
            fixupSize += std::visit(
                Overloaded{
                    [&](const Fixup& fixup) { return fixup.fixupSize; },
                    [&](const auto&) { return static_cast<size_t>(0); },
                },
                column_mapping.translation);
        }

        auto outputTuple = std::make_unique<std::byte[]>(tupleSize);
        auto outputTupleMemory = std::span{outputTuple.get(), tupleSize};
        auto fixupTuple = std::make_unique<std::byte[]>(fixupSize);
        auto fixupTupleMemory = std::span{fixupTuple.get(), fixupSize};

        std::vector<ApplyFixup> applyFixups;
        std::vector<ApplyVarSized> applyVarSized;
        for (const auto& column_mapping : columnMappings)
        {
            SQLBIGINT* nullValueIndicator = nullptr;
            std::byte* nesNullByte = nullptr;
            if (column_mapping.nesType.nullable)
            {
                if (!std::holds_alternative<VarSized>(column_mapping.translation))
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
                            outputTupleMemory.first(column_mapping.nesType.getSizeInBytesWithoutNull()),
                            fixup.fixupFunction);
                        rc = SQLBindCol(
                            hStmt,
                            column_mapping.columnIndex.index,
                            fixup.cType.type,
                            fixupTupleMemory.data(),
                            fixup.fixupSize,
                            nullValueIndicator);
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                        fixupTupleMemory = fixupTupleMemory.subspan(fixup.fixupSize);
                    },
                    [&](const Direct& direct)
                    {
                        rc = SQLBindCol(
                            hStmt,
                            column_mapping.columnIndex.index,
                            direct.cType.type,
                            outputTupleMemory.data(),
                            column_mapping.nesType.getSizeInBytesWithoutNull(),
                            nullValueIndicator);
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                    },
                    [&](const VarSized& varSized)
                    {
                        applyVarSized.emplace_back(
                            column_mapping.columnIndex, varSized.cType, nesNullByte, outputTupleMemory.first(sizeof(VariableSizedAccess)));
                        CHECK(rc, SQL_HANDLE_STMT, hStmt, "SQLBindCol");
                    },
                },
                column_mapping.translation);

            outputTupleMemory = outputTupleMemory.subspan(column_mapping.nesType.getSizeInBytesWithoutNull());
        }


        auto parameterTypes = getSqlParameterTypes(hStmt);
        NES_DEBUG("Binding {} to {}.", fmt::join(parameterTypes, ", "), schema);
        auto timestampLower = std::make_unique<SQL_TIMESTAMP_STRUCT>(converToTimestamp(std::chrono::system_clock::now()));
        auto timestampUpper = std::make_unique<SQL_TIMESTAMP_STRUCT>(converToTimestamp(std::chrono::system_clock::now()));

        /// ColumnSize=26, DecimalDigits=6 → microsecond-precision timestamp
        /// (YYYY-MM-DD HH:MM:SS.ffffff). The previous values of (16, 0)
        /// meant minute precision; that matched the original MSSQL schema
        /// but truncated the bound parameter when used against postgres'
        /// microsecond-precision `timestamp` columns, so the windowed
        /// query returned no rows even when matching data existed.
        constexpr SQLULEN TIMESTAMP_COLUMN_SIZE = 26;
        constexpr SQLSMALLINT TIMESTAMP_DECIMAL_DIGITS = 6;
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
            NULL);
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
            NULL);
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
        SQLRETURN rc = SQLExecute(statement.get());
        CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLExecute");
    }

    bool fetch(TupleBuffer& parent, AbstractBufferProvider& provider)
    {
        auto rc = SQLFetch(statement.get());
        if (rc == SQL_NO_DATA)
        {
            SQLFreeStmt(statement.get(), SQL_CLOSE);
            return false;
        }
        CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLFetch");

        for (const auto& [index, cType, nullable, destination] : varsizedAccesses)
        {
            SQLCHAR probe[1];
            SQLLEN totalLen = 0;
            SQLRETURN rc = SQLGetData(statement.get(), index.index, cType.type, probe, sizeof(probe), &totalLen);
            CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLGetData");


            if (totalLen == SQL_NULL_DATA)
            {
                INVARIANT(nullable, "SQLGetData returned SQL_NULL_DATA for a non nullable column.");
                *nullable = static_cast<std::byte>(1);
                continue;
            }

            if (totalLen == 0)
            {
                *reinterpret_cast<VariableSizedAccess*>(destination.data())
                    = VariableSizedAccess{INVALID_VARIABLE_SIZED_INDEX, VariableSizedAccess::Size{0}};
                continue;
            }

            TupleBuffer resultBuffer;
            if (totalLen == SQL_NO_TOTAL)
            {
                /// SQL Server cannot tell the size of the variable sized data, and i dont have the patience to implement that properly. We will allocate what ever fixed size buffer and attempt to fill or truncate the variable sized data.
                resultBuffer = provider.getBufferBlocking();
                totalLen = resultBuffer.getBufferSize();
            }
            else
            {
                INVARIANT(totalLen >= 0, "SQLGetData returned a negative length: {}.", totalLen);
                /// Account for zero termination
                resultBuffer = provider.getUnpooledBuffer(totalLen + 1).value();
            }

            SQLLEN bytesRead = 0;
            rc = SQLGetData(
                statement.get(),
                index.index,
                cType.type,
                resultBuffer.getAvailableMemoryArea<std::byte>().data(),
                totalLen + 1,
                &bytesRead);
            CHECK(rc, SQL_HANDLE_STMT, statement.get(), "SQLGetData");
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

    std::span<const std::byte> data() const { return {outputTuple.get(), outputTupleSize}; }
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
    , sourceContext(
          std::make_unique<Context>(
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
    NES_DEBUG("ODBCSource connected successfully.");
}

bool waitForTimeout(std::chrono::milliseconds timeout, const std::stop_token& stoken)
{
    std::mutex mtx;
    std::condition_variable_any cv;
    std::unique_lock lock(mtx);
    cv.wait_for(lock, stoken, timeout, [&stoken] { return stoken.stop_requested(); });
    return !stoken.stop_requested();
}

Source::FillTupleBufferResult ODBCSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stoken)
{
    auto currentBuffer = tupleBuffer.getAvailableMemoryArea<std::byte>();
    size_t numberOfTuples = 0;
    bool shouldReturn = false;
    Timestamp watermark = Timestamp(0);

    while (!shouldReturn && !stoken.stop_requested())
    {
        sourceContext->state = std::visit(
            Overloaded{
                [this](ConnectingToDatabaseState&&) -> SourceState
                {
                    Connection connection(sourceContext->connectionInformation);
                    auto statement = PreparedStatement::bind(connection, schema, query);
                    return FetchFromDatabaseState{std::move(connection), std::move(statement)};
                },
                [&](FetchFromDatabaseState&& state) -> SourceState
                {
                    if (!waitForTimeout(std::chrono::milliseconds(pollIntervalMs), stoken))
                    {
                        NES_DEBUG("ODBCSource was requested to stop");
                        return state;
                    }

                    state.statement.exec();
                    auto timestamp = *state.statement.timestampLower;
                    return FetchFromBufferState{
                        std::move(state.connection), std::move(state.statement), Timestamp(toNESTimestamp(getTimestamp(timestamp)))};
                },
                [&](FetchFromBufferState&& state) -> SourceState
                {
                    if (currentBuffer.size() < state.statement.data().size())
                    {
                        watermark = state.watermark;
                        shouldReturn = true;
                        return FetchFromBufferState{std::move(state.connection), std::move(state.statement), std::move(watermark)};
                    }

                    auto hasTuple = state.statement.fetch(tupleBuffer, *bufferProvider);
                    if (!hasTuple)
                    {
                        watermark = Timestamp(toNESTimestamp(getTimestamp(*state.statement.timestampUpper)));
                        shouldReturn = true;
                        return FetchFromDatabaseState{std::move(state.connection), std::move(state.statement)};
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
    const auto driver = config.at(ConfigParametersODBC::DRIVER);
    auto descriptor = DescriptorConfig::validateAndFormat<ConfigParametersODBC>(std::move(config), NAME);
    return descriptor;
}

void ODBCSource::close()
{
}

SourceValidationRegistryReturnType RegisterODBCSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ODBCSource::validateAndFormat(sourceConfig.config);
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterODBCSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<ODBCSource>(sourceRegistryArguments.sourceDescriptor);
}
}
