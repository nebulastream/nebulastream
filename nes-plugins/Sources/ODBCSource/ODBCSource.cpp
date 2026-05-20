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

#include <chrono>
#include <cstddef>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <ODBCConnection.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

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
    , idColumn(sourceDescriptor.getFromConfig(ConfigParametersODBC::ID_COLUMN))
    , query(sourceDescriptor.getFromConfig(ConfigParametersODBC::QUERY))
    , maxRetries(sourceDescriptor.getFromConfig(ConfigParametersODBC::MAX_RETRIES))
    , readOnlyNewRows(sourceDescriptor.getFromConfig(ConfigParametersODBC::READ_ONLY_NEW_ROWS))
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

void ODBCSource::open(std::shared_ptr<AbstractBufferProvider> bufferProviderPtr)
{
    this->connection = std::make_unique<ODBCConnection>();
    this->bufferProvider = std::move(bufferProviderPtr);

    auto connectionString = fmt::format(
        "DRIVER={{{}}};SERVER={};PORT={};DATABASE={};UID={};PWD={};",
        driver,
        this->host,
        this->port,
        this->database,
        this->username,
        this->password);

    /// We don't want to catch errors here, but further up in the query engine
    connection->connect(connectionString, this->syncTable, this->query, this->readOnlyNewRows, this->idColumn);
    this->fetchedSizeOfRow = this->connection->getFetchedSizeOfRow();
    if (this->fetchedSizeOfRow == 0)
    {
        throw CannotOpenSource("Size of schema must not be 0");
    }
    NES_DEBUG("ODBCSource connected successfully.");
}

Source::FillTupleBufferResult ODBCSource::fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token&)
{
    /// fetch as many rows as possible until buffer is full
    const size_t maxRowsPerBuffer = tupleBuffer.getBufferSize() / this->fetchedSizeOfRow;

    auto pollStatus{ODBCPollStatus::NO_NEW_ROWS};
    size_t retryCount = 0;
    while (pollStatus == ODBCPollStatus::NO_NEW_ROWS or retryCount >= maxRetries)
    {
        /// wait for data to arrive
        std::this_thread::sleep_for(std::chrono::milliseconds(pollIntervalMs));
        pollStatus = this->connection->executeQuery(this->query, tupleBuffer, *bufferProvider, maxRowsPerBuffer);
        if (++retryCount >= maxRetries)
        {
            return FillTupleBufferResult::eos();
        }
    }
    /// ODBCConnection advanced TupleBuffer::numberOfTuples as a byte write-cursor; convert it back to a tuple
    /// count. An ODBC source emits already-typed (NATIVE) rows with no downstream input formatter, so the
    /// buffer's numberOfTuples must be the actual row count — SourceThread copies this return value onto it.
    const auto numberOfTuples = tupleBuffer.getNumberOfTuples() / this->fetchedSizeOfRow;
    return FillTupleBufferResult::withBytes(numberOfTuples);
}

DescriptorConfig::Config ODBCSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersODBC>(std::move(config), NAME);
}

void ODBCSource::close()
{
}

SourceValidationRegistryReturnType RegisterODBCSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return ODBCSource::validateAndFormat(std::move(sourceConfig.config));
}

/// The generated SourceGeneratedRegistrar declaration takes SourceRegistryArguments by value; a const-ref definition
/// would not match. The function is reached only by name from the registrar resolver, so it cannot have internal linkage.
/// NOLINTBEGIN(performance-unnecessary-value-param, misc-use-internal-linkage)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterODBCSource(SourceRegistryArguments sourceRegistryArguments)
/// NOLINTEND(performance-unnecessary-value-param, misc-use-internal-linkage)
{
    return std::make_unique<ODBCSource>(sourceRegistryArguments.sourceDescriptor);
}

}
