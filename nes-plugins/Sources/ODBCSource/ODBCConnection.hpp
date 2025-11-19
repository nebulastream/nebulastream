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
#include <Runtime/TupleBuffer.hpp>
#include <utility>
#include <vector>
#include <netdb.h>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <Configurations/Descriptor.hpp>
#include <Sources/SourceDescriptor.hpp>

class ODBCConnection {
private:
    SQLHENV henv = SQL_NULL_HENV;
    SQLHDBC hdbc = SQL_NULL_HDBC;
    SQLHSTMT hstmt = SQL_NULL_HSTMT;

    void checkError(SQLRETURN ret, SQLSMALLINT handleType, SQLHANDLE handle,
                    const std::string& operation) {
        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
            std::string errorMsg = operation + " failed:\n";

            SQLSMALLINT recNum = 1;
            SQLCHAR sqlState[6];
            SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
            SQLINTEGER nativeError;
            SQLSMALLINT textLength;

            // Get all diagnostic records
            while (SQLGetDiagRec(handleType, handle, recNum, sqlState, &nativeError,
                                message, sizeof(message), &textLength) == SQL_SUCCESS) {
                errorMsg += std::format("  [{}] {} (Native Error: {})\n",
                                       reinterpret_cast<char*>(sqlState),
                                       reinterpret_cast<char*>(message),
                                       nativeError);
                recNum++;
            }

            throw std::runtime_error(errorMsg);
        }

        // Print any warnings
        if (ret == SQL_SUCCESS_WITH_INFO) {
            SQLSMALLINT recNum = 1;
            SQLCHAR sqlState[6];
            SQLCHAR message[SQL_MAX_MESSAGE_LENGTH];
            SQLINTEGER nativeError;
            SQLSMALLINT textLength;

            while (SQLGetDiagRec(handleType, handle, recNum, sqlState, &nativeError,
                                message, sizeof(message), &textLength) == SQL_SUCCESS) {
                std::cout << "Warning: [" << reinterpret_cast<char*>(sqlState)
                          << "] " << reinterpret_cast<char*>(message) << '\n';
                recNum++;
            }
        }
    }

public:
    ODBCConnection() {
        setenv("ODBCSYSINI", "/etc", 1);
        setenv("ODBCINI", "/etc/odbc.ini", 1);

        // Allocate environment handle
        SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv);
        if (ret != SQL_SUCCESS) {
            throw std::runtime_error("Failed to allocate environment handle");
        }

        // Set ODBC version
        ret = SQLSetEnvAttr(henv, SQL_ATTR_ODBC_VERSION,
                           reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
        checkError(ret, SQL_HANDLE_ENV, henv, "Set ODBC version");

        // Allocate connection handle
        ret = SQLAllocHandle(SQL_HANDLE_DBC, henv, &hdbc);
        checkError(ret, SQL_HANDLE_ENV, henv, "Allocate connection handle");
    }

    void connect(const std::string& connectionString) {
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
            SQL_DRIVER_NOPROMPT
        );

        checkError(ret, SQL_HANDLE_DBC, hdbc, "Connect to database");

        std::cout << "Successfully connected to MS SQL Server!\n";
        std::cout << "Connection string used: "
                  << reinterpret_cast<char*>(outConnectionString) << '\n';

        // Allocate statement handle
        ret = SQLAllocHandle(SQL_HANDLE_STMT, hdbc, &hstmt);
        checkError(ret, SQL_HANDLE_DBC, hdbc, "Allocate statement handle");
    }

    void executeQuery(const std::string& sqlQuery, NES::TupleBuffer& tupleBuffer) {
        std::cout << "\n========================================\n";
        std::cout << "Executing SQL: " << sqlQuery << '\n';
        std::cout << "========================================\n\n";

        // Create a mutable copy of the query string
        std::vector<SQLCHAR> queryBuffer(sqlQuery.begin(), sqlQuery.end());
        queryBuffer.push_back('\0');

        // Execute the SQL statement
        SQLRETURN ret = SQLExecDirect(
            hstmt,
            queryBuffer.data(),
            static_cast<SQLINTEGER>(sqlQuery.length())
        );
        checkError(ret, SQL_HANDLE_STMT, hstmt, "Execute SQL statement");

        // Get number of columns
        SQLSMALLINT numColumns;
        ret = SQLNumResultCols(hstmt, &numColumns);
        checkError(ret, SQL_HANDLE_STMT, hstmt, "Get number of columns");

        if (numColumns == 0) {
            std::cout << "Query executed successfully (no result set)\n";
            return;
        }

        // Get column information
        std::vector<std::string> columnNames;
        std::vector<SQLSMALLINT> columnTypes;
        std::vector<SQLULEN> columnSizes;

        std::cout << "Result columns:\n";
        for (SQLSMALLINT i = 1; i <= numColumns; i++) {
            SQLCHAR columnName[256];
            SQLSMALLINT nameLength;
            SQLSMALLINT dataType;
            SQLULEN columnSize;
            SQLSMALLINT decimalDigits;
            SQLSMALLINT nullable;

            ret = SQLDescribeCol(
                hstmt,
                i,
                columnName,
                sizeof(columnName),
                &nameLength,
                &dataType,
                &columnSize,
                &decimalDigits,
                &nullable
            );
            checkError(ret, SQL_HANDLE_STMT, hstmt, "Describe column");

            columnNames.push_back(reinterpret_cast<char*>(columnName));
            columnTypes.push_back(dataType);
            columnSizes.push_back(columnSize);

            std::cout << "  Column " << i << ": "
                      << reinterpret_cast<char*>(columnName)
                      << " (Type: " << dataType
                      << ", Size: " << columnSize << ")\n";
        }

        std::cout << "\n";

        // Print column headers
        for (const auto& name : columnNames) {
            std::cout << std::left << std::setw(20) << name << " | ";
        }
        std::cout << '\n';

        for (size_t i = 0; i < columnNames.size(); i++) {
            std::cout << std::string(20, '-') << "-+-";
        }
        std::cout << '\n';

        // Fetch and print rows
        int rowCount = 0;
        while (true) {
            ret = SQLFetch(hstmt);
            if (ret == SQL_NO_DATA) {
                break;
            }
            checkError(ret, SQL_HANDLE_STMT, hstmt, "Fetch row");

            rowCount++;

            // Get data for each column
            for (SQLSMALLINT i = 1; i <= numColumns; i++) {
                // SQLCHAR data[1024];

                if (i % numColumns == 2)
                {
                SQLLEN indicator;
                    int intValue;
                    ret = SQLGetData(
                        hstmt,
                        i,
                        SQL_INTEGER,
                        &intValue,
                        sizeof(int),
                        &indicator
                    );

                    std::string stringValue = std::to_string(intValue);
                    std::string newline = "\n";
                    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                        if (indicator == SQL_NULL_DATA) {
                            std::cout << std::left << std::setw(20) << "NULL" << " | ";
                        } else {
                            // std::string value(reinterpret_cast<char*>(data));
                            if (stringValue.length() > 20) {
                                stringValue = stringValue.substr(0, 17) + "...";
                            }
                            std::cout << std::left << std::setw(20) << stringValue << " | ";
                        }
                    }
                    const auto numTuples = tupleBuffer.getNumberOfTuples();
                    std::memcpy(tupleBuffer.getAvailableMemoryArea<char>().data() + numTuples, stringValue.data(), stringValue.size());
                    std::memcpy(tupleBuffer.getAvailableMemoryArea<char>().data() + numTuples + stringValue.size(), newline.data(), newline.size());
                    tupleBuffer.setNumberOfTuples(numTuples + stringValue.size() + 1);
                }
            }
            std::cout << '\n';
        }

        std::cout << "\nTotal rows: " << rowCount << '\n';

        // Close cursor for next query
        SQLCloseCursor(hstmt);
    }

    ~ODBCConnection() {
        if (hstmt != SQL_NULL_HSTMT) {
            SQLFreeHandle(SQL_HANDLE_STMT, hstmt);
        }
        if (hdbc != SQL_NULL_HDBC) {
            SQLDisconnect(hdbc);
            SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
        }
        if (henv != SQL_NULL_HENV) {
            SQLFreeHandle(SQL_HANDLE_ENV, henv);
        }
    }
};