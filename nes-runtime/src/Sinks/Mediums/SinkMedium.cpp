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

#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
SinkMedium::SinkMedium(SinkFormatPtr sinkFormat, uint32_t numOfProducers, QueryId QueryId)
    : SinkMedium(sinkFormat, numOfProducers, QueryId, 1)
{
}

SinkMedium::SinkMedium(SinkFormatPtr sinkFormat, uint32_t numOfProducers, QueryId queryId, uint64_t numberOfOrigins)
    : sinkFormat(std::move(sinkFormat)), queryId(queryId), numberOfOrigins(numberOfOrigins)
{
    schemaWritten = false;
    NES_ASSERT2_FMT(numOfProducers > 0, "Invalid num of producers on Sink");
}

OperatorId SinkMedium::getOperatorId() const
{
    return INVALID_OPERATOR_ID;
}

uint64_t SinkMedium::getNumberOfWrittenOutBuffers()
{
    std::unique_lock lock(writeMutex);
    return sentBuffer;
}

uint64_t SinkMedium::getNumberOfWrittenOutTuples()
{
    std::unique_lock lock(writeMutex);
    return sentTuples;
}

SchemaPtr SinkMedium::getSchemaPtr() const
{
    return sinkFormat->getSchemaPtr();
}

std::string SinkMedium::getSinkFormat()
{
    return sinkFormat->toString();
}

QueryId SinkMedium::getQueryId() const
{
    return queryId;
}

}
