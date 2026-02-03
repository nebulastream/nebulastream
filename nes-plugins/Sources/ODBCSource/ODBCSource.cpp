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
#include <thread>
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
#include "ODBCConnection.hpp"
#include "SourceRegistry.hpp"
#include "SourceValidationRegistry.hpp"
#include "Util/Strings.hpp"

namespace SQL
{

template <typename Handle>
void checkError(SQLRETURN ret, Handle& handle, std::string_view message)
{
    if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
    {
        return;
    }

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

struct DriverSearchResult
{
    bool sqlSuccess{};
    bool hasDriver{};
};

// DriverSearchResult searchDrivers(const Handle<1, void*>& env, const size_t direction, std::string_view targetDriver)
// {
//     PRECONDITION(direction == 1 or direction == 2, "Allowed search direction are 1(SQL_FETCH_NEXT) and 2(SQL_FETCH_FIRST)");
//     std::array<char, 256> driverDesc;
//     std::array<char, 256> driverAttr;
//     SQLSMALLINT actualDescriptorLength = 0;
//     SQLSMALLINT actualAttributeLength = 0;
//     const SQLRETURN ret = SQLDrivers(
//         *env.env,
//         direction,
//         std::bit_cast<SQLCHAR*>(driverDesc.data()),
//         driverDesc.size() - 1,
//         &actualDescriptorLength,
//         std::bit_cast<SQLCHAR*>(driverAttr.data()),
//         driverAttr.size() - 1,
//         &actualAttributeLength);
//     if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO)
//     {
//         driverDesc[std::min(static_cast<SQLSMALLINT>(driverDesc.size() - 1), actualDescriptorLength)] = '\0';
//         // driverAttr[std::min(static_cast<SQLSMALLINT>(driverAttr.size() - 1), actualAttributeLength)] = '\0';
//         const std::string_view driverDescrView{driverDesc};
//         return DriverSearchResult{.sqlSuccess = true, .hasDriver = (driverDescrView == targetDriver)};
//     }
//     return DriverSearchResult{.sqlSuccess = false, .hasDriver = false};
// }

// bool verifyDriverExists(std::string_view driverName)
// {
//     auto env = EnvironmentHandle::allocate();
//     setEnvironmentAttribute(env, SQL_ATTR_ODBC_VERSION, SQL_OV_ODBC3);
//     auto driverSearchResult = searchDrivers(env, SQL_FETCH_FIRST, driverName);
//     while (driverSearchResult.sqlSuccess and not(driverSearchResult.hasDriver))
//     {
//         driverSearchResult = searchDrivers(env, SQL_FETCH_NEXT, driverName);
//     }
//     return driverSearchResult.hasDriver;
// }
}

namespace NES
{

ODBCSource::~ODBCSource() = default;

ODBCSource::ODBCSource(const SourceDescriptor& sourceDescriptor)
    : host(sourceDescriptor.getFromConfig(ConfigParametersODBC::HOST))
    , port(sourceDescriptor.getFromConfig(ConfigParametersODBC::PORT))
    , database(sourceDescriptor.getFromConfig(ConfigParametersODBC::DATABASE))
    , username(sourceDescriptor.getFromConfig(ConfigParametersODBC::USERNAME))
    , password(sourceDescriptor.getFromConfig(ConfigParametersODBC::PASSWORD))
    , driver(sourceDescriptor.getFromConfig(ConfigParametersODBC::DRIVER))
    , pollIntervalMs(sourceDescriptor.getFromConfig(ConfigParametersODBC::POLL_INTERVAL_MS))
    , syncTable(sourceDescriptor.getFromConfig(ConfigParametersODBC::SYNC_TABLE))
    , query(sourceDescriptor.getFromConfig(ConfigParametersODBC::QUERY))
    , trustServerCertificate(sourceDescriptor.getFromConfig(ConfigParametersODBC::TRUST_SERVER_CERTIFICATE))
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

void ODBCSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    this->connection = std::make_unique<ODBCConnection>();

    auto connectionString = fmt::format(
        "DRIVER={{{}}};SERVER={},{};DATABASE={};UID={};PWD={};TrustServerCertificate={};",
        driver,
        this->host,
        this->port,
        this->database,
        this->username,
        this->password,
        (this->trustServerCertificate) ? "yes" : "no");

    /// We don't want to catch errors here, but further up in the query engine
    connection->connect(connectionString, this->syncTable, this->query);
    this->fetchedSizeOfRow = this->connection->getFetchedSizeOfRow();
    if (this->fetchedSizeOfRow == 0)
    {
        throw CannotOpenSource("Size of schema must not be 0");
    }
    NES_DEBUG("ODBCSource connected successfully.");
}

Source::FillTupleBufferResult ODBCSource::fillTupleBuffer(TupleBuffer& tupleBuffer, AbstractBufferProvider& bufferProvider, const std::stop_token&)
{
    /// fetch as many rows as possible until buffer is full
    const size_t maxRowsPerBuffer = tupleBuffer.getBufferSize() / this->fetchedSizeOfRow;

    ODBCPollStatus pollStatus{ODBCPollStatus::NO_NEW_ROWS};
    size_t retryCount = 0;
    constexpr size_t MAX_RETRIES = 5;
    while (pollStatus == ODBCPollStatus::NO_NEW_ROWS or retryCount >= MAX_RETRIES)
    {
        /// wait for data to arrive
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
        pollStatus = this->connection->executeQuery(this->query, tupleBuffer, bufferProvider, maxRowsPerBuffer);
        if (++retryCount >= MAX_RETRIES)
        {
            return FillTupleBufferResult::eos();
        }
    }
    const auto numberOfTuples = tupleBuffer.getNumberOfTuples() / this->fetchedSizeOfRow;
    return FillTupleBufferResult::withBytes(numberOfTuples);
}

DescriptorConfig::Config ODBCSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    const auto driver = config.at(ConfigParametersODBC::DRIVER);
    auto descriptor = DescriptorConfig::validateAndFormat<ConfigParametersODBC>(std::move(config), NAME);
    // Todo: implement driver check
    (void)driver;
    // if (not SQL::verifyDriverExists(driver))
    // {
    //     throw InvalidConfigParameter("SQL database does not contain driver: {}", driver);
    // }
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
