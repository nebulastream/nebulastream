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

class ODBCConnection
{
private:
    struct TypeInfo
    {
        SQLSMALLINT sqlType;
        NES::DataType::Type nesType;
        size_t nesTypeSize;
    };

    struct FetchedSchema
    {
        size_t numColumns;
        std::vector<TypeInfo> columnTypes;
        std::vector<size_t> columnSizes;
    };

    FetchedSchema fetchedSchema;

    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;

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

    static TypeInfo getTypeInfo(const SQLSMALLINT sqlType)
    {
        static std::unordered_map<SQLSMALLINT, NES::DataType::Type> typeMap
            = {{SQL_CHAR, NES::DataType::Type::CHAR},
               {SQL_VARCHAR, NES::DataType::Type::VARSIZED},
               {SQL_LONGVARCHAR, NES::DataType::Type::VARSIZED},
               {SQL_WCHAR, NES::DataType::Type::UNDEFINED},
               {SQL_WVARCHAR, NES::DataType::Type::UNDEFINED},
               {SQL_WLONGVARCHAR, NES::DataType::Type::UNDEFINED},
               {SQL_DECIMAL, NES::DataType::Type::UNDEFINED},
               {SQL_NUMERIC, NES::DataType::Type::UNDEFINED},
               {SQL_SMALLINT, NES::DataType::Type::INT16},
               {SQL_INTEGER, NES::DataType::Type::INT32},
               {SQL_REAL, NES::DataType::Type::UNDEFINED},
               {SQL_FLOAT, NES::DataType::Type::FLOAT32},
               {SQL_DOUBLE, NES::DataType::Type::FLOAT64},
               {SQL_BIT, NES::DataType::Type::UNDEFINED},
               {SQL_TINYINT, NES::DataType::Type::INT8},
               {SQL_BIGINT, NES::DataType::Type::INT64},
               {SQL_BINARY, NES::DataType::Type::UNDEFINED},
               {SQL_VARBINARY, NES::DataType::Type::UNDEFINED},
               {SQL_LONGVARBINARY, NES::DataType::Type::UNDEFINED},
               {SQL_TYPE_DATE, NES::DataType::Type::UNDEFINED},
               {SQL_TYPE_TIME, NES::DataType::Type::UNDEFINED},
               {SQL_TYPE_TIMESTAMP, NES::DataType::Type::UNDEFINED},
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
            return TypeInfo{.sqlType = sqlType, .nesType = type->second, .nesTypeSize = NES::DataType{type->second}.getSizeInBytes()};
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
            fetchedSchema.columnTypes.emplace_back(getTypeInfo(dataType));
            fetchedSchema.columnSizes.emplace_back(columnSize);
            ++fetchedSchema.numColumns;

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

    void connect(const std::string& connectionString, const std::string_view query)
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
    }

    template <typename T>
    SQLRETURN readVal(const size_t colIdx, NES::TupleBuffer& buffer, const TypeInfo& typeInfo, SQLLEN& indicator)
    {
        PRECONDITION(
            typeInfo.nesTypeSize >= sizeof(T),
            "NES type {} is too small to hold value of sql type: {}",
            magic_enum::enum_name(typeInfo.nesType),
            typeInfo.sqlType);
        T* val = reinterpret_cast<T*>(&buffer.getAvailableMemoryArea<char>()[buffer.getNumberOfTuples()]);
        buffer.setNumberOfTuples(buffer.getNumberOfTuples() + typeInfo.nesTypeSize);
        // Todo: can't simply use 'SQL_CHAR'
        return SQLGetData(hstmt, colIdx, typeInfo.sqlType, val, sizeof(T), &indicator);
    }

    // Todo: create triplet of <SQLType, NESType, Size> <-- maybe size implicit via function
    SQLRETURN readDataIntoBuffer(const size_t colIdx, NES::TupleBuffer& buffer, const TypeInfo& typeInfo, SQLLEN& indicator)
    {
        switch (typeInfo.sqlType)
        {
            case SQL_CHAR: {
                return readVal<char>(colIdx, buffer, typeInfo, indicator);
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
            case SQL_BIGINT: {
                return readVal<int64_t>(colIdx, buffer, typeInfo, indicator);
            }
            case SQL_FLOAT: {
                return readVal<float>(colIdx, buffer, typeInfo, indicator);
            }
            case SQL_DOUBLE: {
                return readVal<double>(colIdx, buffer, typeInfo, indicator);
            }
            // Todo: need buffer manager here
            case SQL_VARCHAR: {
                throw NES::UnknownDataType("Not supporting {} type in ODBC source", typeInfo.sqlType);
            }
            // Todo: need buffer manager here
            case SQL_LONGVARCHAR: {
                throw NES::UnknownDataType("Not supporting {} type in ODBC source", typeInfo.sqlType);
            }
            case SQL_REAL:
            case SQL_BIT:
            case SQL_BINARY:
            case SQL_VARBINARY:
            case SQL_LONGVARBINARY:
            /// WCHAR/WVARCHAR/WLONGVARCHAR contain unicode data
            case SQL_WCHAR:
            case SQL_WVARCHAR:
            case SQL_WLONGVARCHAR:
            case SQL_DECIMAL:
            case SQL_NUMERIC:
            case SQL_TYPE_DATE:
            case SQL_TYPE_TIME:
            case SQL_TYPE_TIMESTAMP:
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

    void executeQuery(std::vector<SQLCHAR> queryBuffer, NES::TupleBuffer& tupleBuffer)
    {
        // Execute the SQL statement
        SQLRETURN ret = SQLExecDirect(hstmt, queryBuffer.data(), static_cast<SQLINTEGER>(queryBuffer.size()));
        checkError(ret, SQL_HANDLE_STMT, hstmt, "Execute SQL statement");

        int rowCount = 0;
        while (true)
        {
            ret = SQLFetch(hstmt);
            if (ret == SQL_NO_DATA)
            {
                break;
            }
            checkError(ret, SQL_HANDLE_STMT, hstmt, "Fetch row");

            /// Process fetched row
            for (const auto [columnIdx, columnType] : fetchedSchema.columnTypes | NES::views::enumerate)
            {
                SQLLEN indicator{};
                /// ODBC expects column indexes starting at 1
                readDataIntoBuffer(columnIdx + 1, tupleBuffer, columnType, indicator);
                if (ret != SQL_SUCCESS and ret != SQL_SUCCESS_WITH_INFO)
                {
                    throw NES::UnknownDataType(
                        "Not supporting {} type in ODBC source", magic_enum::enum_name(columnType.nesType));
                }
                if (indicator == SQL_NULL_DATA)
                {
                    throw NES::UnknownDataType("Not supporting null types in ODBC source");
                }
            }
            rowCount++;
        }

        // Close cursor for next query
        SQLCloseCursor(hstmt);
    }

    ~ODBCConnection()
    {
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
};