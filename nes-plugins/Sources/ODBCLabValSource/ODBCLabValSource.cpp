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

#include <ODBCLabValSource.hpp>

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
#include <Util/Strings.hpp>
#include <fmt/ranges.h>
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <ODBCConnection.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

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

}

namespace NES
{

ODBCLabValSource::~ODBCLabValSource() = default;

ODBCLabValSource::ODBCLabValSource(const SourceDescriptor& sourceDescriptor)
    : host(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::HOST))
    , port(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::PORT))
    , database(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::DATABASE))
    , username(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::USERNAME))
    , password(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::PASSWORD))
    , driver(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::DRIVER))
    , pollIntervalMs(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::POLL_INTERVAL_MS))
    , syncTable(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::SYNC_TABLE))
    , query(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::QUERY))
    , trustServerCertificate(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::TRUST_SERVER_CERTIFICATE))
    , maxRetries(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::MAX_RETRIES))
    , readOnlyNewRows(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::READ_ONLY_NEW_ROWS))
    , useCheckpoint(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::USE_CHECKPOINT))
    , logTuples(sourceDescriptor.getFromConfig(ConfigParametersODBCLabVal::LOG_TUPLES))
{
}

std::ostream& ODBCLabValSource::toString(std::ostream& str) const
{
    str << "\nODBCLabValSource(";
    str << "\n  generated tuples: " << this->generatedTuples;
    str << "\n  generated buffers: " << this->generatedBuffers;
    str << ")\n";
    return str;
}

void ODBCLabValSource::open(std::shared_ptr<AbstractBufferProvider> bufferProviderPtr)
{
    this->connection = std::make_unique<ODBCConnection>();
    this->bufferProvider = std::move(bufferProviderPtr);

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
    connection->connect(connectionString, this->syncTable, this->query, this->readOnlyNewRows, this->useCheckpoint);
    this->fetchedSizeOfRow = this->connection->getFetchedSizeOfRow();
    if (this->fetchedSizeOfRow == 0)
    {
        throw CannotOpenSource("Size of schema must not be 0");
    }
    NES_DEBUG("ODBCLabValSource connected successfully.");
}

Source::FillTupleBufferResult ODBCLabValSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stop)
{
    /// fetch as many rows as possible until buffer is full
    const size_t maxRowsPerBuffer = tupleBuffer.getBufferSize() / this->fetchedSizeOfRow;

    auto pollStatus{ODBCPollStatus::NO_NEW_ROWS};
    size_t retryCount = 0;
    while (pollStatus == ODBCPollStatus::NO_NEW_ROWS or retryCount >= maxRetries)
    {
        if (stop.stop_requested())
        {
            NES_DEBUG("Stopping ODBC Source with query: {}", this->query);
            return FillTupleBufferResult::eos();
        }
        /// wait for data to arrive
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
        pollStatus = this->connection->executeQuery(this->query, tupleBuffer, *bufferProvider, maxRowsPerBuffer, this->logTuples);

        if (++retryCount >= maxRetries)
        {
            NES_DEBUG("Reached max retries of {}", maxRetries);
            return FillTupleBufferResult::eos();
        }
    }
    const auto numberOfTuples = tupleBuffer.getNumberOfTuples() / this->fetchedSizeOfRow;
    return FillTupleBufferResult::withBytes(numberOfTuples);
}

DescriptorConfig::Config ODBCLabValSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    const auto driver = config.at(ConfigParametersODBCLabVal::DRIVER);
    auto descriptor = DescriptorConfig::validateAndFormat<ConfigParametersODBCLabVal>(std::move(config), NAME);
    return descriptor;
}

void ODBCLabValSource::close()
{
}

SourceValidationRegistryReturnType RegisterODBCLabValSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ODBCLabValSource::validateAndFormat(sourceConfig.config);
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterODBCLabValSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<ODBCLabValSource>(sourceRegistryArguments.sourceDescriptor);
}

}
