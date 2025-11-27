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

#include <array>
#include <chrono>
#include <cstring>
#include <exception>
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
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include "Util/Ranges.hpp"

enum class ODBCPollStatus : uint8_t
{
    NO_NEW_ROWS,
    FEWER_ROWS,
    NEW_ROWS,
};

class ODBCConnection
{
    struct TypeInfo
    {
        SQLSMALLINT sqlType;
        NES::DataType::Type nesType;
        size_t nesTypeSize;
        size_t sqlColumnSize;
    };

    struct FetchedSchema
    {
        size_t numColumns;
        std::vector<TypeInfo> columnTypes;
        size_t sizeOfRow;
    };

    void checkError(SQLRETURN ret, SQLSMALLINT handleType, SQLHANDLE handle, const std::string& operation)
    {
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
        {
            std::string errorMsg = operation + " failed:\n";

            SQLSMALLINT recNum = 1;
            SQLCHAR sqlState[6];
            SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
            SQLINTEGER nativeError;
            SQLSMALLINT textLength;

            // Get all diagnostic records
            while (SQLGetDiagRec(handleType, handle, recNum, sqlState, &nativeError, message, sizeof(message), &textLength) == SQL_SUCCESS)
            {
                errorMsg += std::format(
                    "  [{}] {} (Native Error: {})\n", reinterpret_cast<char*>(sqlState), reinterpret_cast<char*>(message), nativeError);
                recNum++;
            }

            throw std::runtime_error(errorMsg);
        }

        // Print any warnings
        if (ret == SQL_SUCCESS_WITH_INFO)
        {
            SQLSMALLINT recNum = 1;
            SQLCHAR sqlState[6];
            SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
            SQLINTEGER nativeError;
            SQLSMALLINT textLength;

            while (SQLGetDiagRec(handleType, handle, recNum, sqlState, &nativeError, message, sizeof(message), &textLength) == SQL_SUCCESS)
            {
                std::cout << "Warning: [" << reinterpret_cast<char*>(sqlState) << "] " << reinterpret_cast<char*>(message) << '\n';
                recNum++;
            }
        }
    }

    static TypeInfo getTypeInfo(const SQLSMALLINT sqlType, const SQLULEN sqlColLen)
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
                    .nesTypeSize = sizeof(NES::VariableSizedAccess),
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

public:
    ODBCConnection()
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

    size_t getFetchedSizeOfRow() const { return fetchedSchema.sizeOfRow; }

    void fetchColumns(std::string_view connectionString)
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
                fetchedSchema.sizeOfRow +
                fetchedSchema.columnTypes.back().nesTypeSize);
            fetchedSchema.sizeOfRow += fetchedSchema.columnTypes.back().nesTypeSize;

            // Todo: validation of types and names (requires schema)
            NES_DEBUG(
                "Fetching column {}, with name: {} and type: {}, max digits/bytes: {}, is nullable: {}",
                i,
                std::string_view(reinterpret_cast<char*>(&colName), nameLength),
                // magic_enum::enum_name(fetchedSchema.columnTypes.back()),
                magic_enum::enum_name(fetchedSchema.columnTypes.back().nesType),
                columnSize,
                nullable)
        }
    }

    size_t syncRowCount()
    {
        SQLRETURN ret
            = SQLExecDirect(hstmtCount, reinterpret_cast<SQLCHAR*>(countQuery.data()), static_cast<SQLINTEGER>(countQuery.size()));
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

    void connect(const std::string& connectionString, const std::string_view syncTable, const std::string_view query)
    {
        SQLCHAR outConnectionString[1024];
        SQLSMALLINT outConnectionStringLength;

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
        this->rowCountTracker = syncRowCount();
    }

    template <typename T>
    SQLRETURN readVal(const size_t colIdx, NES::TupleBuffer& buffer, const TypeInfo& typeInfo, SQLLEN& indicator)
    {
        T* val = reinterpret_cast<T*>(&buffer.getAvailableMemoryArea<char>()[buffer.getNumberOfTuples()]);
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + typeInfo.nesTypeSize);
        return SQLGetData(hstmt, colIdx, typeInfo.sqlType, val, typeInfo.nesTypeSize, &indicator);
    }

    SQLRETURN readVarSized(
        const size_t colIdx,
        const TypeInfo& typeInfo,
        SQLLEN& indicator,
        NES::TupleBuffer& buffer,
        NES::AbstractBufferProvider& bufferProvider)
    {
        /// Add 1 byte for the null terminator (required)
        if (auto varSizedBuffer = bufferProvider.getUnpooledBuffer(typeInfo.sqlColumnSize + sizeof(uint32_t)))
        {
            const auto ret = SQLGetData(
                hstmt,
                colIdx,
                SQL_C_CHAR,
                varSizedBuffer.value().getAvailableMemoryArea<char>().data() + sizeof(uint32_t),
                typeInfo.sqlColumnSize,
                &indicator);
            checkError(ret, SQL_HANDLE_STMT, hstmt, "Execute SQL statement");
            INVARIANT(indicator != SQL_NULL_DATA, "not supporting null data for varsized");
            varSizedBuffer.value().getAvailableMemoryArea<uint32_t>()[0] = indicator;
            const auto childBufferIdx = buffer.storeChildBuffer(varSizedBuffer.value());
            *reinterpret_cast<NES::VariableSizedAccess*>(&buffer.getAvailableMemoryArea<char>()[buffer.getNumberOfTuples()])
                = NES::VariableSizedAccess{childBufferIdx};
            buffer.setNumberOfTuples(buffer.getNumberOfTuples() + NES::DataType{NES::DataType::Type::VARSIZED}.getSizeInBytes());
            return ret;
        }
        throw NES::BufferAllocationFailure("Could not get unpooled buffer for VarSized value");
    }

    uint64_t timestampStructToUnix(const SQL_TIMESTAMP_STRUCT& ts)
    {
        using namespace std::chrono;

        // Create a time_point from the timestamp components
        auto date = sys_days{year{ts.year} / month{ts.month} / day{ts.day}};
        auto time_of_day = hours{ts.hour} + minutes{ts.minute} + seconds{ts.second};
        auto tp = date + time_of_day;

        // Convert to Unix timestamp (seconds since epoch)
        auto unix_seconds = duration_cast<seconds>(tp.time_since_epoch()).count();

        return static_cast<uint64_t>(unix_seconds);
    }

    // Todo: we could leave all of the 'parsing/formatting' work to 'nes-input-formatters'
    //       IF: we allowed the source to add metadata (e.g., to the buffer) that tells the input formatter, for a specific buffer,
    //           where the first tuple starts and the last tuple ends.
    //       IF: there is some way to access the binary data directly
    //       IF: there is some way to handle binary column data in the input formatter
    SQLRETURN readDataIntoBuffer(
        const size_t colIdx,
        const TypeInfo& typeInfo,
        SQLLEN& indicator,
        NES::TupleBuffer& buffer,
        NES::AbstractBufferProvider& bufferProvider)
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
            case SQL_WVARCHAR: {
                return readVarSized(colIdx, typeInfo, indicator, buffer, bufferProvider);
            }
            case SQL_VARCHAR: {
                return readVarSized(colIdx, typeInfo, indicator, buffer, bufferProvider);
            }
            case SQL_LONGVARCHAR: {
                return readVarSized(colIdx, typeInfo, indicator, buffer, bufferProvider);
            }
            case SQL_VARBINARY: {
                return readVarSized(colIdx, typeInfo, indicator, buffer, bufferProvider);
            }
            case SQL_LONGVARBINARY: {
                return readVarSized(colIdx, typeInfo, indicator, buffer, bufferProvider);
            }
            case SQL_TYPE_TIMESTAMP: {
                SQL_TIMESTAMP_STRUCT timestamp;
                const auto ret = SQLGetData(hstmt, colIdx, SQL_TYPE_TIMESTAMP, &timestamp, sizeof(SQL_TIMESTAMP_STRUCT), &indicator);
                const uint64_t timestampAsUint64 = timestampStructToUnix(timestamp);
                *reinterpret_cast<uint64_t*>(&buffer.getAvailableMemoryArea<char>()[buffer.getNumberOfTuples()]) = timestampAsUint64;
                buffer.setNumberOfTuples(buffer.getNumberOfTuples() + typeInfo.nesTypeSize);
                return ret;
            }
            /// WCHAR/WVARCHAR/WLONGVARCHAR contain unicode data
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
                throw NES::UnknownDataType("No support for {} types in ODBC source", typeInfo.sqlType);
        }
    }

    std::vector<SQLCHAR> buildNewRowFetchSting(const std::string_view userQuery, const uint64_t numRowsToFetch)
    {
        const auto trimmedQuery = NES::Util::trimWhiteSpaces(userQuery);
        const auto selectSplit = NES::Util::splitWithStringDelimiter<std::string_view>(trimmedQuery, "SELECT");
        INVARIANT(selectSplit.size() == 1, "Query '{}' did not contain exactly one SELECT statement at the start.", trimmedQuery);
        std::string selectTopNRows = fmt::format("SELECT TOP {} {} ORDER BY LabVal_ID DESC", numRowsToFetch, selectSplit.at(0));
        std::vector<SQLCHAR> queryBuffer(selectTopNRows.begin(), selectTopNRows.end());
        queryBuffer.push_back('\0');
        return queryBuffer;
    }

    ODBCPollStatus executeQuery(
        std::string_view query, NES::TupleBuffer& tupleBuffer, NES::AbstractBufferProvider& bufferProvider, const size_t rowsPerBuffer)
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
            if (ret == SQL_NO_DATA)
            {
                NES_DEBUG("No more data in table after reading {} rows.", totalRowCount);
                // noMoreData = true;
                break;
            }
            checkError(ret, SQL_HANDLE_STMT, hstmt, "Fetch row");

            /// Process fetched row
            for (const auto [columnIdx, columnType] : fetchedSchema.columnTypes | NES::views::enumerate)
            {
                SQLLEN indicator{};
                /// ODBC expects column indexes starting at 1
                readDataIntoBuffer(columnIdx + 1, columnType, indicator, tupleBuffer, bufferProvider);
                if (ret != SQL_SUCCESS and ret != SQL_SUCCESS_WITH_INFO)
                {
                    throw NES::UnknownDataType("Not supporting {} type in ODBC source", magic_enum::enum_name(columnType.nesType));
                }
                if (indicator == SQL_NULL_DATA)
                {
                    throw NES::UnknownDataType("Not supporting null types in ODBC source");
                }
            }
            ++totalRowCount;
        }
        SQLCloseCursor(hstmt);
        return ODBCPollStatus::NEW_ROWS;
    }

    ~ODBCConnection()
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

private:
    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;
    SQLHSTMT hstmtCount = SQL_NULL_HSTMT;
    FetchedSchema fetchedSchema;
    size_t totalRowCount{0};
    uint64_t rowCountTracker{0};
    std::string countQuery;
};