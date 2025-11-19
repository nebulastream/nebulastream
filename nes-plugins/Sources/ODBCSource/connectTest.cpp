#include <iostream>
#include <format>
#include <sql.h>
#include <sqlext.h>
#include <string>
#include <stdexcept>
#include <cstdlib>
#include <vector>
#include <iomanip>

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

    void executeQuery(const std::string& sqlQuery) {
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
                SQLCHAR data[1024];
                SQLLEN indicator;

                ret = SQLGetData(
                    hstmt,
                    i,
                    SQL_C_CHAR,
                    data,
                    sizeof(data),
                    &indicator
                );

                if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
                    if (indicator == SQL_NULL_DATA) {
                        std::cout << std::left << std::setw(20) << "NULL" << " | ";
                    } else {
                        std::string value(reinterpret_cast<char*>(data));
                        if (value.length() > 20) {
                            value = value.substr(0, 17) + "...";
                        }
                        std::cout << std::left << std::setw(20) << value << " | ";
                    }
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

void open()
{
    try {
        // Set environment variables for ODBC configuration
        ODBCConnection conn;

        // Connection string
        std::string connectionString =
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost,1433;"
            "DATABASE=master;"
            "UID=sa;"
            "PWD=samplePassword1!;"
            "TrustServerCertificate=yes;";

        conn.connect(connectionString);

        std::cout << "Connection successful!\n\n";

        // Example queries
        // Query 1: Your custom view
        conn.executeQuery("SELECT * FROM dbo.v_Vitalwerte_unvalidiert");

        // Query 2: Get SQL Server version (commented out - uncomment if needed)
        // conn.executeQuery("SELECT @@VERSION AS ServerVersion");

        // Query 3: Get database list (commented out - uncomment if needed)
        // conn.executeQuery("SELECT name, database_id, create_date FROM sys.databases ORDER BY name");

        // You can add more queries here or make it interactive
        // For example, to query a specific table:
        // conn.executeQuery("SELECT TOP 10 * FROM YourTableName");

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << '\n';
    }
}

int main() {
    open();
    return 0;
}