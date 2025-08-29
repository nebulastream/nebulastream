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

#include <Aggregation/Function/Synopsis/SynopsisFunctionRef.hpp>

namespace NES
{

SampleProbePhysicalOperator::SampleProbePhysicalOperator(
    const Schema& sampleSchema,
    const Record::RecordFieldIdentifier& inputFieldIdentifier,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , sampleSchema(sampleSchema)
    , inputFieldIdentifier(inputFieldIdentifier)
{
}

void SampleProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto sampleRef = SynopsisFunctionRef(sampleSchema);
    // TODO Finish ;)
    (void)recordBuffer;
    //sampleRef.initializeForReading(recordBuffer.read(inputFieldIdentifier).cast<VariableSizedData>().getReference());

    const auto sampleSize = Util::readValueFromMemRef<uint64_t>(sampleRef.getMetaData().getContent());
    for (auto i = nautilus::val<uint64_t>(0); i < sampleSize; ++i)
    {
        auto sampleRecord = sampleRef.readNextRecord();
        //sampleRecord.reassignFields(recordBuffer);
        child->execute(executionCtx, sampleRecord);
    }
}

}
