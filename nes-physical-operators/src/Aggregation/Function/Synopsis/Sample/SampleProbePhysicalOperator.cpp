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

#include <Aggregation/Function/Synopsis/Sample/SampleProbePhysicalOperator.hpp>

#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/Synopsis/SynopsisFunctionRef.hpp>
#include <function.hpp>

namespace NES
{

SampleProbePhysicalOperator::SampleProbePhysicalOperator(
    const Schema& sampleSchema, const Record::RecordFieldIdentifier& fieldIdentifier, WindowMetaData windowMetaData)
    : sampleSchema(sampleSchema)
    , inputFieldIdentifier(fieldIdentifier)
    , windowMetaData(windowMetaData)
{
}

void SampleProbePhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    auto sampleRef = SynopsisFunctionRef(sampleSchema);
    sampleRef.initializeForReading(record.read(inputFieldIdentifier).cast<VariableSizedData>().getReference());
    const auto sampleSize = Nautilus::Util::readValueFromMemRef<uint64_t>(sampleRef.getMetaData().getContent());
    for (auto i = nautilus::val<uint64_t>(0); i < sampleSize; ++i)
    {
        auto sampleRecord = sampleRef.readNextRecord();
        // TODO Also add ID if group by? Maybe save that info?
        sampleRecord.write(windowMetaData.windowStartFieldName, record.read(windowMetaData.windowStartFieldName));
        sampleRecord.write(windowMetaData.windowEndFieldName, record.read(windowMetaData.windowEndFieldName));
        executeChild(executionCtx, sampleRecord);
    }
}

std::optional<PhysicalOperator> SampleProbePhysicalOperator::getChild() const
{
    return child;
}

void SampleProbePhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}


}
