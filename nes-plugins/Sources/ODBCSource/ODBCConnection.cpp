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

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <format>
#include <memory>
#include <ostream>
#include <ranges>
#include <stop_token>
#include <string>
#include <string_view>
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
#include <Sources/SourceDescriptor.hpp>
#include <Util/Ranges.hpp>
#include <ErrorHandling.hpp>
#include <ODBCConnection.hpp>
#include "Util/Strings.hpp"

namespace
{

uint64_t timestampStructToUnix(const SQL_TIMESTAMP_STRUCT& ts)
{
    using namespace std::chrono;

    // Create a time_point from the timestamp components
    const auto date = sys_days{year{ts.year} / month{ts.month} / day{ts.day}};
    const auto timeOfDay = hours{ts.hour} + minutes{ts.minute} + seconds{ts.second};
    const auto tp = date + timeOfDay;

    // Convert to Unix timestamp (seconds since epoch)
    const auto unix_seconds = duration_cast<seconds>(tp.time_since_epoch()).count();

    return static_cast<uint64_t>(unix_seconds);
}

void checkError(const SQLRETURN ret, const SQLSMALLINT handleType, const SQLHANDLE handle, const std::string& operation)
{
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        std::stringstream errorMsg;
        errorMsg << operation << " failed:\n";

        SQLSMALLINT recNum = 1;
        SQLCHAR sqlState[6];
        SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
        SQLINTEGER nativeError;
        SQLSMALLINT textLength;

        // Get all diagnostic records
        while (SQLGetDiagRec(handleType, handle, recNum, sqlState, &nativeError, message, sizeof(message), &textLength) == SQL_SUCCESS)
        {
            errorMsg << std::format(
                "  [{}] {} (Native Error: {})\n", reinterpret_cast<char*>(sqlState), reinterpret_cast<char*>(message), nativeError);
            recNum++;
        }

        throw NES::RunningRoutineFailure("{}", errorMsg.str());
    }

    // Print any warnings
    if (ret == SQL_SUCCESS_WITH_INFO)
    {
        SQLSMALLINT recNum = 1;
        std::array<unsigned char, 6> sqlState{};
        std::array<unsigned char, SQL_MAX_MESSAGE_LENGTH> message{};
        SQLINTEGER nativeError;
        SQLSMALLINT textLength;

        while (SQLGetDiagRec(handleType, handle, recNum, sqlState.data(), &nativeError, message.data(), sizeof(message), &textLength)
               == SQL_SUCCESS)
        {
            NES_DEBUG("Warning: [{}] {}", reinterpret_cast<char*>(sqlState.data()), reinterpret_cast<char*>(message.data()));
            recNum++;
        }
    }
}

TypeInfo getTypeInfo(const SQLSMALLINT sqlType, const SQLULEN sqlColLen)
{
    static std::unordered_map<SQLSMALLINT, NES::DataType::Type> typeMap
        = {{SQL_CHAR, NES::DataType::Type::CHAR},
           {SQL_VARCHAR, NES::DataType::Type::VARSIZED},
           {SQL_LONGVARCHAR, NES::DataType::Type::VARSIZED},
           {SQL_SMALLINT, NES::DataType::Type::INT16},
           {SQL_INTEGER, NES::DataType::Type::INT32},
           {SQL_FLOAT, NES::DataType::Type::FLOAT32},
           {SQL_DOUBLE, NES::DataType::Type::FLOAT64},
           {SQL_TINYINT, NES::DataType::Type::INT8},
           {SQL_BIGINT, NES::DataType::Type::INT64},
           {SQL_REAL, NES::DataType::Type::FLOAT32},
           {SQL_VARBINARY, NES::DataType::Type::VARSIZED},
           {SQL_WCHAR, NES::DataType::Type::UNDEFINED},
           {SQL_WVARCHAR, NES::DataType::Type::VARSIZED},
           {SQL_WLONGVARCHAR, NES::DataType::Type::UNDEFINED},
           {SQL_DECIMAL, NES::DataType::Type::UNDEFINED},
           {SQL_NUMERIC, NES::DataType::Type::UNDEFINED},
           {SQL_BIT, NES::DataType::Type::BOOLEAN},
           {SQL_BINARY, NES::DataType::Type::UNDEFINED},
           {SQL_LONGVARBINARY, NES::DataType::Type::UNDEFINED},
           {SQL_TYPE_DATE, NES::DataType::Type::UNDEFINED},
           {SQL_TYPE_TIME, NES::DataType::Type::UNDEFINED},
           {SQL_TYPE_TIMESTAMP, NES::DataType::Type::UINT64},
           {SQL_INTERVAL_MONTH, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_YEAR, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_YEAR_TO_MONTH, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_DAY, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_HOUR, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_MINUTE, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_SECOND, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_DAY_TO_HOUR, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_DAY_TO_MINUTE, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_DAY_TO_SECOND, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_HOUR_TO_MINUTE, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_HOUR_TO_SECOND, NES::DataType::Type::UNDEFINED},
           {SQL_INTERVAL_MINUTE_TO_SECOND, NES::DataType::Type::UNDEFINED},
           {SQL_GUID, NES::DataType::Type::UNDEFINED}};
    if (const auto type = typeMap.find(sqlType); type != typeMap.end())
    {
        if (type->second == NES::DataType::Type::VARSIZED)
        {
            return TypeInfo{
                .sqlType = SQL_VARCHAR,
                .nesType = type->second,
                .nesTypeSize = NES::DataType{NES::DataType::Type::VARSIZED}.getSizeInBytes(),
                .sqlColumnSize = sqlColLen};
        }
        constexpr size_t maxDigitsIn32BitFloat = 24;
        if (type->second == NES::DataType::Type::FLOAT32 and sqlColLen > maxDigitsIn32BitFloat)
        {
            return TypeInfo{
                .sqlType = SQL_DOUBLE,
                .nesType = NES::DataType::Type::FLOAT64,
                .nesTypeSize = NES::DataType{NES::DataType::Type::FLOAT64}.getSizeInBytes(),
                .sqlColumnSize = sqlColLen};
        }
        if (type->second == NES::DataType::Type::INT64)
        {
            return TypeInfo{
                .sqlType = SQL_C_SBIGINT,
                .nesType = NES::DataType::Type::INT64,
                .nesTypeSize = NES::DataType{NES::DataType::Type::INT64}.getSizeInBytes(),
                .sqlColumnSize = sqlColLen};
        }
        return TypeInfo{
            .sqlType = sqlType,
            .nesType = type->second,
            .nesTypeSize = NES::DataType{type->second}.getSizeInBytes(),
            .sqlColumnSize = sqlColLen};
    }
    throw NES::CannotOpenSource("Cannot conver SQLType {} to NES type.", sqlType);
}
}

namespace NES
{

ODBCConnection::ODBCConnection()
{
    setenv("ODBCSYSINI", "/etc", 1);
    setenv("ODBCINI", "/etc/odbc.ini", 1);

    // Allocate environment handle
    SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
    if (ret != SQL_SUCCESS)
    {
        throw std::runtime_error("Failed to allocate environment handle");
    }

    // Set ODBC version
    ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION, reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
    checkError(ret, SQL_HANDLE_ENV, henv, "Set ODBC version");

    // Allocate connection handle
    ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
    checkError(ret, SQL_HANDLE_ENV, henv, "Allocate connection handle");
}

ODBCConnection::~ODBCConnection()
{
    SQLCloseCursor(hstmt);

    if (hstmt != SQL_NULL_HSTMT)
    {
        SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
    }
    if (hdbc != SQL_NULL_HDBC)
    {
        SQLDisconnect(hdbc);
        SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
    }
    if (henv != SQL_NULL_HENV)
    {
        SQLFreeHandle(SQL_HANDLE_ENV, henv);
    }
}

void ODBCConnection::fetchColumns(std::string_view connectionString)
{
    if (const SQLRETURN ret = SQLPrepare(hstmt, reinterpret_cast<SQLCHAR*>(const_cast<char*>(connectionString.data())), SQL_NTS);
        !SQL_SUCCEEDED(ret))
    {
        checkError(ret, SQL_HANDLE_STMT, hstmt, "Prepare failed");
    }

    SQLSMALLINT numCols = 0;
    SQLNumResultCols(hstmt, &numCols);

    for (SQLSMALLINT i = 1; i <= numCols; i++)
    {
        SQLCHAR colName[256];
        SQLSMALLINT nameLength = 0;
        SQLSMALLINT dataType = 0;
        SQLULEN columnSize = 0;
        SQLSMALLINT decimalDigits = 0;
        SQLSMALLINT nullable = 0;

        SQLDescribeCol(hstmt, i, colName, sizeof(colName), &nameLength, &dataType, &columnSize, &decimalDigits, &nullable);
        // fetchedSchema.columnTypes.emplace_back(getColumnType(dataType));
        fetchedSchema.columnTypes.emplace_back(getTypeInfo(dataType, columnSize));
        ++fetchedSchema.numColumns;
        NES_WARNING(
            "Increasing odbc schema size from {} by {} to {}",
            fetchedSchema.sizeOfRow,
            fetchedSchema.columnTypes.back().nesTypeSize,
            fetchedSchema.sizeOfRow + fetchedSchema.columnTypes.back().nesTypeSize);
        fetchedSchema.sizeOfRow += fetchedSchema.columnTypes.back().nesTypeSize;

        // Todo: validation of types and names (requires schema)
        NES_DEBUG(
            "Fetching column {}, with name: {} and type: {}, max digits/bytes: {}, is nullable: {}",
            i,
            std::string_view(reinterpret_cast<char*>(&colName), nameLength),
            magic_enum::enum_name(fetchedSchema.columnTypes.back().nesType),
            columnSize,
            nullable)
    }
}

size_t ODBCConnection::syncRowCount()
{
    SQLRETURN ret = SQLExecDirect(hstmtCount, reinterpret_cast<SQLCHAR*>(countQuery.data()), static_cast<SQLINTEGER>(countQuery.size()));
    checkError(ret, SQL_HANDLE_STMT, hstmtCount, "Execute COUNT statement");

    uint64_t rowCount{0};
    ret = SQLFetch(hstmtCount);
    if (ret == SQL_NO_DATA)
    {
        NES_ERROR("Failed to count rows of table: {}.", countQuery);
        return rowCount;
    }
    checkError(ret, SQL_HANDLE_STMT, hstmtCount, "Fetch row");

    /// Process fetched row
    SQLLEN indicator{};
    ret = SQLGetData(hstmtCount, 1, SQL_C_UBIGINT, &rowCount, sizeof(uint64_t), &indicator);
    checkError(ret, SQL_HANDLE_STMT, hstmtCount, "Get row count data");
    SQLCloseCursor(hstmtCount);
    return rowCount;
}

void ODBCConnection::connect(const std::string& connectionString, const std::string_view syncTable, const std::string_view query, const bool readOnlyNewRows)
{
    SQLCHAR outConnectionString[1024];
    SQLSMALLINT outConnectionStringLength;

    NES_DEBUG("Attempting connection with: {}",  connectionString);
    std::cout << "Attempting connection with: " << connectionString << '\n';

    SQLRETURN ret = SQLDriverConnect(
        hdbc,
        nullptr,
        reinterpret_cast<SQLCHAR*>(const_cast<char*>(connectionString.c_str())),
        SQL_NTS,
        outConnectionString,
        sizeof(outConnectionString),
        &outConnectionStringLength,
        SQL_DRIVER_NOPROMPT);

    checkError(ret, SQL_HANDLE_DBC, hdbc, "Connect to database");

    std::cout << "Successfully connected to MS SQL Server!\n";
    std::cout << "Connection string used: " << reinterpret_cast<char*>(outConnectionString) << '\n';
    NES_DEBUG("Successfully connected to MS SQL Server!");
    NES_DEBUG("Connection string used: {}", reinterpret_cast<char*>(outConnectionString));

    // Allocate statement handle
    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
    checkError(ret, SQL_HANDLE_DBC, hdbc, "Allocate statement handle");
    fetchColumns(query);
    // Todo: fetch row count
    this->countQuery = fmt::format("SELECT COUNT(*) FROM {};", syncTable);

    ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmtCount);
    checkError(ret, SQL_HANDLE_DBC, hdbc, "Allocate count sstatement handle");

    if (const SQLRETURN ret = SQLPrepare(hstmtCount, reinterpret_cast<SQLCHAR*>(countQuery.data()), SQL_NTS); !SQL_SUCCEEDED(ret))
    {
        checkError(ret, SQL_HANDLE_STMT, hstmtCount, "Prepare failed");
    }
    /// fetch the current number of rows to read only newly added rows
    if (readOnlyNewRows)
    {
        this->rowCountTracker = syncRowCount();
    }
}

SQLRETURN ODBCConnection::readVarSized(
    const size_t colIdx, const TypeInfo& typeInfo, SQLLEN& indicator, TupleBuffer& buffer, AbstractBufferProvider& bufferProvider) const
{
    /// Add 1 byte for the null terminator (required)
    if (auto varSizedBuffer = bufferProvider.getUnpooledBuffer(typeInfo.sqlColumnSize))
    {
        const auto ret = SQLGetData(
            hstmt,
            colIdx,
            SQL_C_CHAR,
            varSizedBuffer.value().getAvailableMemoryArea<char>().data(),
            typeInfo.sqlColumnSize,
            &indicator);
        checkError(ret, SQL_HANDLE_STMT, hstmt, "Execute SQL statement");
        INVARIANT(indicator != SQL_NULL_DATA, "not supporting null data for varsized");
        const auto childBufferIdx = buffer.storeChildBuffer(varSizedBuffer.value());
        *reinterpret_cast<VariableSizedAccess*>(&buffer.getAvailableMemoryArea<char>()[buffer.getNumberOfTuples()])
            = VariableSizedAccess{childBufferIdx, VariableSizedAccess::Size{static_cast<uint64_t>(indicator)}};
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + DataType{DataType::Type::VARSIZED}.getSizeInBytes());
        return ret;
    }

    throw BufferAllocationFailure("Could not get unpooled buffer for VarSized value");
}

SQLRETURN ODBCConnection::readDataIntoBuffer(
    const size_t colIdx, const TypeInfo& typeInfo, SQLLEN& indicator, TupleBuffer& buffer, AbstractBufferProvider& bufferProvider) const
{
    switch (typeInfo.sqlType)
    {
        case SQL_BINARY: {
            return readVal<char>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_BIT: {
            return readVal<bool>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_CHAR: {
            std::array<char, 2> charBuffer = {};
            const auto ret = SQLGetData(hstmt, colIdx, SQL_C_CHAR, charBuffer.data(), charBuffer.size(), &indicator);
            buffer.getAvailableMemoryArea<char>()[buffer.getNumberOfTuples()] = charBuffer[0];
            buffer.setNumberOfTuples(buffer.getNumberOfTuples() + typeInfo.nesTypeSize);
            return ret;
        }
        case SQL_TINYINT: {
            return readVal<int8_t>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_SMALLINT: {
            return readVal<int16_t>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_INTEGER: {
            return readVal<int32_t>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_C_SBIGINT: {
            return readVal<int64_t>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_FLOAT: {
            return readVal<float>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_DOUBLE: {
            return readVal<double>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_REAL: {
            return readVal<float>(colIdx, buffer, typeInfo, indicator);
        }
        case SQL_TYPE_TIMESTAMP: {
            SQL_TIMESTAMP_STRUCT timestamp;
            const auto ret = SQLGetData(hstmt, colIdx, SQL_TYPE_TIMESTAMP, &timestamp, sizeof(SQL_TIMESTAMP_STRUCT), &indicator);
            const uint64_t timestampAsUint64 = timestampStructToUnix(timestamp);
            *reinterpret_cast<uint64_t*>(&buffer.getAvailableMemoryArea<char>()[buffer.getNumberOfTuples()]) = timestampAsUint64;
            buffer.setNumberOfTuples(buffer.getNumberOfTuples() + typeInfo.nesTypeSize);
            return ret;
        }
        /// VarChars (WCHAR/WVARCHAR/WLONGVARCHAR contain unicode data)
        case SQL_WVARCHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY: {
            return readVarSized(colIdx, typeInfo, indicator, buffer, bufferProvider);
        }
        case SQL_WCHAR:
        case SQL_WLONGVARCHAR:
        case SQL_DECIMAL:
        case SQL_NUMERIC:
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
        case SQL_INTERVAL_MONTH:
        case SQL_INTERVAL_YEAR:
        case SQL_INTERVAL_YEAR_TO_MONTH:
        case SQL_INTERVAL_DAY:
        case SQL_INTERVAL_HOUR:
        case SQL_INTERVAL_MINUTE:
        case SQL_INTERVAL_SECOND:
        case SQL_INTERVAL_DAY_TO_HOUR:
        case SQL_INTERVAL_DAY_TO_MINUTE:
        case SQL_INTERVAL_DAY_TO_SECOND:
        case SQL_INTERVAL_HOUR_TO_MINUTE:
        case SQL_INTERVAL_HOUR_TO_SECOND:
        case SQL_INTERVAL_MINUTE_TO_SECOND:
        case SQL_GUID:
        default:
            throw UnknownDataType("No support for {} types in ODBC source", typeInfo.sqlType);
    }
}

std::vector<SQLCHAR> ODBCConnection::buildNewRowFetchSting(const std::string_view userQuery, const uint64_t numRowsToFetch)
{
    const auto trimmedQuery = trimWhiteSpaces(userQuery);
    const auto selectSplit = splitWithStringDelimiter<std::string_view>(trimmedQuery, "SELECT");
    INVARIANT(selectSplit.size() == 1, "Query '{}' did not contain exactly one SELECT statement at the start.", trimmedQuery);
    std::string selectTopNRows = fmt::format("SELECT TOP {} {} ORDER BY LabVal_ID DESC", numRowsToFetch, selectSplit.at(0));
    std::vector<SQLCHAR> queryBuffer(selectTopNRows.begin(), selectTopNRows.end());
    queryBuffer.push_back('\0');
    return queryBuffer;
}

ODBCPollStatus ODBCConnection::executeQuery(
    const std::string_view query, TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const size_t rowsPerBuffer)
{
    const auto newRowCount = syncRowCount();
    if (newRowCount <= this->rowCountTracker)
    {
        return (newRowCount < this->rowCountTracker) ? ODBCPollStatus::FEWER_ROWS : ODBCPollStatus::NO_NEW_ROWS;
    }

    /// Create query string that fetches only the last N rows
    const auto rowsToFetch = std::min(newRowCount - this->rowCountTracker, rowsPerBuffer);
    auto queryBuffer = buildNewRowFetchSting(query, rowsToFetch);

    // There are new rows, Execute the SQL statement
    SQLRETURN ret = SQLExecDirect(hstmt, queryBuffer.data(), static_cast<SQLINTEGER>(queryBuffer.size()));
    checkError(ret, SQL_HANDLE_STMT, hstmt, "Execute SQL statement");

    this->rowCountTracker += rowsToFetch;
    for (size_t bufferLocalRowCount = 0; bufferLocalRowCount < rowsToFetch; ++bufferLocalRowCount)
    {
        ret = SQLFetch(hstmt);
        INVARIANT(ret != SQL_NO_DATA, "Ran out of data in polling ODBC source, which should never happen.");
        checkError(ret, SQL_HANDLE_STMT, hstmt, "Fetch row");

        /// Process fetched row
        for (const auto [columnIdx, columnType] : fetchedSchema.columnTypes | NES::views::enumerate)
        {
            SQLLEN indicator{};
            /// ODBC column indexes start at >1<
            readDataIntoBuffer(columnIdx + 1, columnType, indicator, tupleBuffer, bufferProvider);
            if (ret != SQL_SUCCESS and ret != SQL_SUCCESS_WITH_INFO)
            {
                throw UnknownDataType("Not supporting {} type in ODBC source", magic_enum::enum_name(columnType.nesType));
            }
            if (indicator == SQL_NULL_DATA)
            {
                throw UnknownDataType("Not supporting null types in ODBC source");
            }
        }
    }
    SQLCloseCursor(hstmt);
    return ODBCPollStatus::NEW_ROWS;
}
}