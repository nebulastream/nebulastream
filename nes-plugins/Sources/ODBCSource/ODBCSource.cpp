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

#include "ODBCSource.hpp"

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
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ranges.h>
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include "SourceRegistry.hpp"
#include "SourceValidationRegistry.hpp"

namespace SQL
{

/// Helper function to escape a CSV field (e.g., handle quotes and commas)
std::string escapeCSVField(const std::string& field)
{
    bool needsQuotes = field.find_first_of(",\"\r\n") != std::string::npos;
    std::string escaped = field;

    /// Replace " with ""
    size_t pos = 0;
    while ((pos = escaped.find('\"', pos)) != std::string::npos)
    {
        escaped.replace(pos, 1, "\"\"");
        pos += 2; /// Skip over the added quote
    }

    if (needsQuotes)
    {
        escaped = "\"" + escaped + "\"";
    }

    return escaped;
}

/// Function to convert a fetched row to a CSV-formatted string
std::string convertRowToCSV(SQLHSTMT stmt, SQLSMALLINT numCols)
{
    std::vector<std::string> rowData;

    for (SQLSMALLINT col = 1; col <= numCols; col++)
    {
        SQLCHAR buffer[1024]; /// Adjust buffer size as needed
        SQLLEN indicator;
        SQLRETURN ret = SQLGetData(stmt, col, SQL_C_CHAR, buffer, sizeof(buffer), &indicator);

        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
        {
            /// Handle errors (e.g., log and return empty string)
            rowData.push_back("ERROR");
            continue;
        }

        if (indicator == SQL_NULL_DATA)
        {
            rowData.push_back("NULL");
            continue;
        }

        /// Convert buffer to string and escape for CSV
        std::string value(reinterpret_cast<char*>(buffer));
        rowData.push_back(escapeCSVField(value));
    }

    /// Join columns with commas
    std::string csvRow;
    for (size_t i = 0; i < rowData.size(); i++)
    {
        csvRow += rowData[i];
        if (i < rowData.size() - 1)
        {
            csvRow += ",";
        }
    }

    return csvRow;
}

/// Function to map SQL data type codes to human-readable names
const char* sqlTypeToString(SQLSMALLINT dataType)
{
    static std::unordered_map<SQLSMALLINT, const char*> typeMap
        = {{SQL_CHAR, "SQL_CHAR"},
           {SQL_VARCHAR, "SQL_VARCHAR"},
           {SQL_LONGVARCHAR, "SQL_LONGVARCHAR"},
           {SQL_WCHAR, "SQL_WCHAR"},
           {SQL_WVARCHAR, "SQL_WVARCHAR"},
           {SQL_WLONGVARCHAR, "SQL_WLONGVARCHAR"},
           {SQL_DECIMAL, "SQL_DECIMAL"},
           {SQL_NUMERIC, "SQL_NUMERIC"},
           {SQL_SMALLINT, "SQL_SMALLINT"},
           {SQL_INTEGER, "SQL_INTEGER"},
           {SQL_REAL, "SQL_REAL"},
           {SQL_FLOAT, "SQL_FLOAT"},
           {SQL_DOUBLE, "SQL_DOUBLE"},
           {SQL_BIT, "SQL_BIT"},
           {SQL_TINYINT, "SQL_TINYINT"},
           {SQL_BIGINT, "SQL_BIGINT"},
           {SQL_BINARY, "SQL_BINARY"},
           {SQL_VARBINARY, "SQL_VARBINARY"},
           {SQL_LONGVARBINARY, "SQL_LONGVARBINARY"},
           {SQL_TYPE_DATE, "SQL_TYPE_DATE"},
           {SQL_TYPE_TIME, "SQL_TYPE_TIME"},
           {SQL_TYPE_TIMESTAMP, "SQL_TYPE_TIMESTAMP"},
           {SQL_INTERVAL_MONTH, "SQL_INTERVAL_MONTH"},
           {SQL_INTERVAL_YEAR, "SQL_INTERVAL_YEAR"},
           {SQL_INTERVAL_YEAR_TO_MONTH, "SQL_INTERVAL_YEAR_TO_MONTH"},
           {SQL_INTERVAL_DAY, "SQL_INTERVAL_DAY"},
           {SQL_INTERVAL_HOUR, "SQL_INTERVAL_HOUR"},
           {SQL_INTERVAL_MINUTE, "SQL_INTERVAL_MINUTE"},
           {SQL_INTERVAL_SECOND, "SQL_INTERVAL_SECOND"},
           {SQL_INTERVAL_DAY_TO_HOUR, "SQL_INTERVAL_DAY_TO_HOUR"},
           {SQL_INTERVAL_DAY_TO_MINUTE, "SQL_INTERVAL_DAY_TO_MINUTE"},
           {SQL_INTERVAL_DAY_TO_SECOND, "SQL_INTERVAL_DAY_TO_SECOND"},
           {SQL_INTERVAL_HOUR_TO_MINUTE, "SQL_INTERVAL_HOUR_TO_MINUTE"},
           {SQL_INTERVAL_HOUR_TO_SECOND, "SQL_INTERVAL_HOUR_TO_SECOND"},
           {SQL_INTERVAL_MINUTE_TO_SECOND, "SQL_INTERVAL_MINUTE_TO_SECOND"},
           {SQL_GUID, "SQL_GUID"}};

    auto it = typeMap.find(dataType);
    if (it != typeMap.end())
    {
        return it->second;
    }
    else
    {
        return "UNKNOWN";
    }
}

template <typename Handle>
void checkError(SQLRETURN ret, Handle& handle, std::string_view message)
{
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
        return;

    SQLCHAR sqlState[6];
    SQLCHAR messageText[SQL_MAX_MESSAGE_LENGTH];
    SQLSMALLINT textLength;
    SQLINTEGER nativeError;

    SQLGetDiagRec(Handle::Type, *handle.env, 1, sqlState, &nativeError, messageText, SQL_MAX_MESSAGE_LENGTH, &textLength);
    throw NES::CannotOpenSource(
        "Cannot open ODBC Source: {}. {} (SQL State: {})",
        message,
        std::string(std::bit_cast<char*>(&messageText), textLength),
        std::string(std::bit_cast<char*>(&sqlState), 6));
}

template <size_t TypeValue, typename HandleType, typename... Args>
struct Handle
{
    constexpr static SQLSMALLINT Type = TypeValue;
    std::optional<HandleType> env = {};
    static Handle allocate(const Args&... args)
    {
        HandleType env;
        SQLRETURN ret;
        if constexpr (sizeof...(args) > 0)
        {
            ret = SQLAllocHandle(Type, (*args.env)..., &env);
        }
        else
        {
            ret = SQLAllocHandle(Type, SQL_NULL_HANDLE, &env);
        }

        if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
        {
            Handle tmp{env};
            checkError(ret, tmp, "Cannot allocate handle");
            tmp.env.reset();
        }
        return Handle{env};
    }

    operator bool() const { return env.has_value(); }

    Handle(const Handle& other) = delete;
    Handle(Handle&& other) noexcept { std::swap(env, other.env); }
    Handle& operator=(const Handle& other) = delete;
    Handle& operator=(Handle&& other) noexcept
    {
        if (env)
        {
            std::cerr << "Freeing handle: " << Type << std::endl;
            SQLFreeHandle(Type, *env);
            env.reset();
        }

        std::swap(env, other.env);
        return *this;
    }

    ~Handle()
    {
        if (env)
        {
            std::cerr << "Freeing handle: " << Type << std::endl;
            SQLFreeHandle(Type, *env);
        }
    }

private:
    explicit Handle(HandleType env) : env(env) { }
};

using EnvironmentHandle = Handle<SQL_HANDLE_ENV, SQLHENV>;
using ConnectionHandle = Handle<SQL_HANDLE_DBC, SQLHDBC, EnvironmentHandle>;
using StatementHandle = Handle<SQL_HANDLE_STMT, SQLHSTMT, ConnectionHandle>;

void setEnvironmentAttribute(EnvironmentHandle& handle, SQLINTEGER attribute, size_t value)
{
    const SQLRETURN ret = SQLSetEnvAttr(*handle.env, attribute, std::bit_cast<void*>(value), 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        throw NES::CannotOpenSource("Failed to set environment attribute: {}", attribute);
    }
}

void setConnectionAttribute(ConnectionHandle& handle, SQLINTEGER attribute, size_t value)
{
    const SQLRETURN ret = SQLSetConnectAttr(*handle.env, attribute, std::bit_cast<void*>(value), 0);
    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO)
    {
        throw NES::CannotOpenSource("Failed to set connection attribute: {}", attribute);
    }
}

size_t getNumberOfResultColumns(StatementHandle& handle)
{
    SQLSMALLINT numCols;
    SQLNumResultCols(*handle.env, &numCols);
    return numCols;
}

class ColumnDescriptor
{
    std::string name;
    SQLSMALLINT dataType = 0, decimalDigits = 0, nullable = 0;
    SQLULEN colSize = 0;

public:
    static std::vector<ColumnDescriptor> cols(StatementHandle& stmt)
    {
        auto numberOfColumns = getNumberOfResultColumns(stmt);
        std::vector<ColumnDescriptor> result(numberOfColumns);

        for (size_t i = 0; i < numberOfColumns; ++i)
        {
            std::array<SQLCHAR, 256> colName = {};
            SQLSMALLINT colNameLen;
            auto ret = SQLDescribeCol(
                *stmt.env,
                i + 1, /// 1-Based Index
                colName.data(),
                colName.size(),
                &colNameLen,
                &result[i].dataType,
                &result[i].colSize,
                &result[i].decimalDigits,
                &result[i].nullable);
            result[i].name = {std::bit_cast<char*>(colName.data()), static_cast<size_t>(colNameLen)};
            checkError(ret, stmt, "Failed to fetch column description");
        }

        return result;
    }

    friend auto format_as(const ColumnDescriptor& columnDescriptor);
};

auto format_as(const ColumnDescriptor& columnDescriptor)
{
    return fmt::format("{} {}", columnDescriptor.name, sqlTypeToString(columnDescriptor.dataType));
}

class DriverDescription
{
    std::array<char, 256> driverDesc;
    std::array<char, 256> driverAttr;

public:
    static std::vector<DriverDescription> drivers(EnvironmentHandle& env)
    {
        std::vector<DriverDescription> result;
        result.emplace_back();
        auto& descriptor = result.back();
        SQLSMALLINT actualDescriptorLength = 0;
        SQLSMALLINT actualAttributeLength = 0;
        SQLRETURN ret = SQLDrivers(
            *env.env,
            SQL_FETCH_FIRST,
            std::bit_cast<SQLCHAR*>(descriptor.driverDesc.data()),
            static_cast<SQLSMALLINT>(descriptor.driverDesc.size() - 1),
            &actualDescriptorLength,
            std::bit_cast<SQLCHAR*>(descriptor.driverAttr.data()),
            static_cast<SQLSMALLINT>(descriptor.driverAttr.size() - 1),
            &actualAttributeLength);

        descriptor.driverDesc[std::min(static_cast<SQLSMALLINT>(descriptor.driverDesc.size() - 1), actualDescriptorLength)] = '\0';
        descriptor.driverAttr[std::min(static_cast<SQLSMALLINT>(descriptor.driverAttr.size() - 1), actualAttributeLength)] = '\0';


        while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
        {
            result.emplace_back();
            auto& descriptor = result.back();
            SQLSMALLINT actualDescriptorLength = 0;
            SQLSMALLINT actualAttributeLength = 0;
            ret = SQLDrivers(
                *env.env,
                SQL_FETCH_NEXT,
                std::bit_cast<SQLCHAR*>(descriptor.driverDesc.data()),
                descriptor.driverDesc.size() - 1,
                &actualDescriptorLength,
                std::bit_cast<SQLCHAR*>(descriptor.driverAttr.data()),
                descriptor.driverAttr.size() - 1,
                &actualAttributeLength);
            descriptor.driverDesc[std::min(static_cast<SQLSMALLINT>(descriptor.driverDesc.size() - 1), actualDescriptorLength)] = '\0';
            descriptor.driverAttr[std::min(static_cast<SQLSMALLINT>(descriptor.driverAttr.size() - 1), actualAttributeLength)] = '\0';
        }

        result.pop_back();
        return result;
    }

    std::string_view description() { return {driverDesc.data()}; }
    std::string_view attr() { return {driverAttr.data()}; }
};

void verifyDriverExists(std::string_view driverName)
{
    auto env = EnvironmentHandle::allocate();

    setEnvironmentAttribute(env, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3);

    auto drivers = DriverDescription::drivers(env);
    auto it = std::ranges::find(drivers, driverName, &DriverDescription::description);
    if (it == drivers.end())
    {
        throw NES::InvalidConfigParameter(
            "Driver {} not found.\nExisting Drivers are: [{}]",
            driverName,
            fmt::join(std::views::transform(drivers, &DriverDescription::description), ", "));
    }
}
}

namespace NES::Sources
{

struct Context
{
    SQL::EnvironmentHandle env;
    SQL::ConnectionHandle connection;
    SQL::StatementHandle statement;
    size_t numberOfCols;
    std::string latest;

    ~Context()
    {
        {
            auto _ = std::move(statement);
        }
        {
            if (connection)
            {
                std::cerr << "Disconnecting" << std::endl;
                SQLDisconnect(*connection.env);
            }
            auto _ = std::move(connection);
        }
        {
            auto _ = std::move(env);
        }
    }
};

ODBCSource::~ODBCSource() = default;

ODBCSource::ODBCSource(const SourceDescriptor& sourceDescriptor)
    : host(sourceDescriptor.getFromConfig(ConfigParametersODBC::HOST))
    , database(sourceDescriptor.getFromConfig(ConfigParametersODBC::DATABASE))
    , username(sourceDescriptor.getFromConfig(ConfigParametersODBC::USERNAME))
    , password(sourceDescriptor.getFromConfig(ConfigParametersODBC::PASSWORD))
    , driver(sourceDescriptor.getFromConfig(ConfigParametersODBC::DRIVER))
    , query(sourceDescriptor.getFromConfig(ConfigParametersODBC::QUERY))
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


void ODBCSource::open()
{
    auto env = SQL::EnvironmentHandle::allocate();
    setEnvironmentAttribute(env, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3);

    /// Allocate connection handle
    auto dbc = SQL::ConnectionHandle::allocate(env);
    auto connectionString = fmt::format(
        "DRIVER={{{}}};SERVER={};DATABASE={};UID={};PWD={};TrustServerCertificate=yes;",
        driver,
        this->host,
        this->database,
        this->username,
        this->password);

    setConnectionAttribute(dbc, SQL_ATTR_LOGIN_TIMEOUT, 10);
    auto ret
        = SQLDriverConnect(*dbc.env, NULL, std::bit_cast<SQLCHAR*>(connectionString.c_str()), SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
    checkError(ret, dbc, "Failed to connect to Driver");

    size_t numberOfCols = 0;
    {
        auto schemaVerification = SQL::StatementHandle::allocate(dbc);
        ret = SQLExecDirect(*schemaVerification.env, std::bit_cast<SQLCHAR*>(query.c_str()), SQL_NTS);
        checkError(ret, schemaVerification, "Failed to execute query");
        auto cols = SQL::ColumnDescriptor::cols(schemaVerification);
        numberOfCols = cols.size();
        NES_INFO("Schema:\n\t{}", fmt::join(cols, ",\n\t"))
    }

    /// Allocate statement handle
    auto statement = SQL::StatementHandle::allocate(dbc);
    ret = SQLExecDirect(*statement.env, std::bit_cast<SQLCHAR*>(query.c_str()), SQL_NTS);
    checkError(ret, statement, "Failed to execute query");

    context = std::make_unique<Context>(std::move(env), std::move(dbc), std::move(statement), numberOfCols);
}

size_t ODBCSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
{
    size_t totalBytes = 0;
    if (!context->latest.empty())
    {
        const auto truncatedNumberOfBytes = std::min(context->latest.size(), tupleBuffer.getBufferSize());
        memcpy(tupleBuffer.getBuffer(), context->latest.data(), truncatedNumberOfBytes);

        if (context->latest.size() > truncatedNumberOfBytes)
        {
            context->latest = context->latest.substr(truncatedNumberOfBytes);
            return truncatedNumberOfBytes;
        }
        context->latest = {};
    }

    while (SQLFetch(*context->statement.env) == SQL_SUCCESS)
    {
        auto row = SQL::convertRowToCSV(*context->statement.env, static_cast<SQLSMALLINT>(context->numberOfCols));
        size_t bytesLeft = tupleBuffer.getBufferSize() - totalBytes;
        size_t bytesToWrite = std::min(bytesLeft, row.size());
        memcpy(tupleBuffer.getBuffer() + totalBytes, row.c_str(), bytesToWrite);
        totalBytes += bytesToWrite;

        if (bytesLeft < row.size() + 1)
        {
            context->latest = row.substr(bytesToWrite);
            context->latest += "\n";
            break;
        }
        else
        {
            *(tupleBuffer.getBuffer() + totalBytes) = '\n';
            totalBytes++;
        }
    }

    return totalBytes;
}


std::unique_ptr<Configurations::DescriptorConfig::Config> ODBCSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    auto descriptor = Configurations::DescriptorConfig::validateAndFormat<ConfigParametersODBC>(std::move(config), NAME);
    if (descriptor)
    {
        auto driver = config.at(ConfigParametersODBC::DRIVER);
        SQL::verifyDriverExists(driver);
    }

    return descriptor;
}

void ODBCSource::close()
{
    this->context.reset();
}

std::unique_ptr<SourceValidationRegistryReturnType>
SourceValidationGeneratedRegistrar::RegisterODBCSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ODBCSource::validateAndFormat(sourceConfig.config);
}

std::unique_ptr<SourceRegistryReturnType> SourceGeneratedRegistrar::RegisterODBCSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<ODBCSource>(sourceRegistryArguments.sourceDescriptor);
}

}
