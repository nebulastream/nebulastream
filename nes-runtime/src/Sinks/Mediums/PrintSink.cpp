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

#include <sstream>
#include <string>
#include <utility>
#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/PrintSink.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES
{
PrintSink::PrintSink(
    SinkFormatPtr format,
    Runtime::NodeEnginePtr nodeEngine,
    uint32_t numOfProducers,
    SharedQueryId sharedQueryId,
    DecomposedQueryPlanId decomposedQueryPlanId,
    std::ostream& pOutputStream,
    uint64_t numberOfOrigins)
    : SinkMedium(std::move(format), std::move(nodeEngine), numOfProducers, sharedQueryId, decomposedQueryPlanId, numberOfOrigins)
    , outputStream(pOutputStream)
{
}

PrintSink::~PrintSink() = default;

SinkMediumTypes PrintSink::getSinkMediumType()
{
    return SinkMediumTypes::PRINT_SINK;
}

bool PrintSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef)
{
    std::unique_lock lock(writeMutex);
    NES_DEBUG("PrintSink: getSchema medium  {}  format  {}", toString(), sinkFormat->toString());

    if (!inputBuffer)
    {
        throw Exceptions::RuntimeException("PrintSink::writeData input buffer invalid");
    }

    NES_TRACE("PrintSink::getData: write data");
    auto buffer = sinkFormat->getFormattedBuffer(inputBuffer);
    NES_TRACE("PrintSink::getData: write buffer of size  {}", buffer.size());
    outputStream << buffer << std::endl;
    return true;
}

std::string PrintSink::toString() const
{
    std::stringstream ss;
    ss << "PRINT_SINK(";
    ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << ")";
    ss << ")";
    return ss.str();
}

void PrintSink::setup()
{
    // currently not required
}
void PrintSink::shutdown()
{
    // currently not required
}

} // namespace NES
