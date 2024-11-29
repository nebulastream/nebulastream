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

#include <API/AttributeField.hpp>
#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {

IJOperatorHandler::IJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                     const OriginId outputOriginId,
                                     const int64_t lowerBound,
                                     const int64_t upperBound,
                                     const SchemaPtr& leftSchema,
                                     const SchemaPtr& rightSchema,
                                     const uint64_t leftPageSize,
                                     const uint64_t rightPageSize)
    : pageSizeRight(rightPageSize), pageSizeLeft(leftPageSize), numberOfWorkerThreads(1), outputOriginId(outputOriginId),
      lowerBound(lowerBound), upperBound(upperBound), sizeOfRecordLeft(leftSchema->getSchemaSizeInBytes()),
      sizeOfRecordRight(rightSchema->getSchemaSizeInBytes()), leftSchema(leftSchema), rightSchema(rightSchema),
      watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins)), sequenceNumber(1) {}

void IJOperatorHandler::start(PipelineExecutionContextPtr pipelineCtx, uint32_t) {
    NES_INFO("Started IJOperatorHandler!");
    setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
    setBufferManager(pipelineCtx->getBufferManager());
    this->rightTuples->emplace_back(
        std::make_unique<Nautilus::Interface::PagedVectorVarSized>(bufferManager, this->rightSchema, this->pageSizeRight));
}

void IJOperatorHandler::setNumberOfWorkerThreads(uint64_t numberOfWorkerThreads) {
    if (IJOperatorHandler::alreadySetup) {
        NES_DEBUG("IJOperatorHandler::setup was called already!");
        return;
    }
    IJOperatorHandler::alreadySetup = true;

    NES_DEBUG("IJOperatorHandler::setup was called!");
    IJOperatorHandler::numberOfWorkerThreads = numberOfWorkerThreads;
}

void IJOperatorHandler::emitIntervalToProbe(auto& interval, PipelineExecutionContext* pipelineCtx) {

    if (interval->getNumberOfTuplesRight() >= 1) {
        // Gets new empty pooled tupleBuffer from BufferPoolManager
        auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
        // Returns the buffer memory of the tuple buffer as an EmittedNLJWindowTriggerTask (containing information for a join window trigger)
        auto bufferMemory = tupleBuffer.getBuffer<EmittedIJTriggerTask>();
        // set information in buffer
        bufferMemory->intervalIdentifier = interval->getId();
        tupleBuffer.setNumberOfTuples(interval->getNumberOfTuplesRight() + 1);

        /** As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
         *  The watermark can not be the slice end as some buffer might be still waiting for getting processed.
         */
        // operator id that creates this buffer
        tupleBuffer.setOriginId(getOutputOriginId());
        tupleBuffer.setSequenceData({getNextSequenceNumber(), /*chunkNumber*/ 1, true});
        tupleBuffer.setWatermark(interval->getIntervalStart());

        interval->intervalState = IJIntervalInfoState::EMITTED_TO_PROBE;
        // The tupleBuffer is emitted to the Query manager, where a new task is created for it
        pipelineCtx->dispatchBuffer(tupleBuffer);

        NES_DEBUG("Emitted intervalId {} with watermarkTs {} sequenceNumber {} originId {} for no. right tuples {}, the total "
                  "number of tuples is {}",
                  bufferMemory->intervalIdentifier,
                  tupleBuffer.getWatermark(),
                  tupleBuffer.getSequenceNumber(),
                  tupleBuffer.getOriginId(),
                  interval->getNumberOfTuplesRight(),
                  tupleBuffer.getNumberOfTuples());
    } else {
        NES_INFO("Set tombstone for interval cause there were no right tuples present in interval {}", interval->toString());
        interval->intervalState = IJIntervalInfoState::MARKED_FOR_DELETION;
    }
}

uint64_t IJOperatorHandler::createAndAppendNewInterval(int64_t lowerIntervalBound, int64_t upperIntervalBound) {
    auto currentIntervalsLocked = currentIntervals.wlock();
    auto newInterval = std::make_shared<IJInterval>(lowerIntervalBound,
                                                    upperIntervalBound,
                                                    numberOfWorkerThreads,
                                                    bufferManager,
                                                    IJOperatorHandler::leftSchema,
                                                    IJOperatorHandler::pageSizeLeft,
                                                    IJOperatorHandler::rightSchema,
                                                    IJOperatorHandler::pageSizeRight);

    auto newIntervalId = newInterval->getId();
    auto it = std::find_if(currentIntervalsLocked->begin(),
                           currentIntervalsLocked->end(),
                           [&newIntervalId](std::shared_ptr<IJInterval> obj) {
                               return obj->getId() == newIntervalId;
                           });

    if (it == currentIntervalsLocked->end()) {
        NES_DEBUG("interval {} is not contained in the opHandler list of current intervals, will be append to current intervals",
                  newInterval->toString())
        currentIntervalsLocked->push_back(newInterval);
    }
    currentIntervalsLocked.unlock();
    return newIntervalId;
}

void IJOperatorHandler::updateWatermarkForWorker(uint64_t watermark, WorkerThreadId workerThreadId) {
    workerThreadIdToWatermarkMap[workerThreadId] = watermark;
}

uint64_t IJOperatorHandler::getMinWatermarkForWorker() {
    auto minVal = std::min_element(workerThreadIdToWatermarkMap.begin(),
                                   workerThreadIdToWatermarkMap.end(),
                                   [](const auto& l, const auto& r) {
                                       return l.second < r.second;
                                   });
    return minVal == workerThreadIdToWatermarkMap.end() ? -1 : minVal->second;
}

uint64_t IJOperatorHandler::getNumberOfCurrentIntervals() { return this->currentIntervals->size(); }

std::string IJOperatorHandler::toString() {
    std::ostringstream basicOstringstream;
    basicOstringstream << "(Current Lower Bound: " << lowerBound << " Current Upper Bound: " << upperBound
                       << " currentIntervals: " << currentIntervals->size() << ")";
    return basicOstringstream.str();
}

void* IJOperatorHandler::getPagedVectorRefRight(WorkerThreadId workerThreadId) {
    const auto pos = workerThreadId % rightTuples->size();
    return rightTuples->at(pos).get();
}

void IJOperatorHandler::checkAndTriggerIntervals(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx) {
    // The watermark processor handles the minimal watermark across both streams
    uint64_t newGlobalWatermark =
        watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    int64_t smallestIntervalStart = std::numeric_limits<int64_t>::max();

    NES_DEBUG("newGlobalWatermark {} bufferMetaData {} ", newGlobalWatermark, bufferMetaData.toString());
    auto currentIntervalsLocked = currentIntervals.wlock();
    for (IJIntervalPtr interval : *currentIntervalsLocked) {
        // interval end is in future or interval is already emitted, skip this interval
        if (interval->getIntervalEnd() > static_cast<int>(newGlobalWatermark)
            || interval->intervalState == IJIntervalInfoState::EMITTED_TO_PROBE) {
            NES_DEBUG("The interval {} can not be triggered yet or has already been triggered", interval->getId());
            continue;
        }
        if (interval->getIntervalStart() < smallestIntervalStart) {
            smallestIntervalStart = interval->getIntervalStart();
        }
        // we can simply emit as interval is does not need to be combined with other intervals
        emitIntervalToProbe(interval, pipelineCtx);
    }
    if (smallestIntervalStart < std::numeric_limits<int64_t>::max()) {
        smallestIntervalStartEmitted = smallestIntervalStart;
    }
}

std::optional<IJIntervalPtr> IJOperatorHandler::getIntervalByIntervalIdentifier(uint64_t intervalIdentifier) {
    auto currentIntervalLocked = currentIntervals.rlock();
    for (auto& curInterval : *currentIntervalLocked) {
        if (curInterval->getId() == intervalIdentifier) {
            return curInterval;
        }
    }
    return std::nullopt;
}

void IJOperatorHandler::triggerAllIntervals(PipelineExecutionContext* pipelineCtx) {
    auto currentIntervalLocked = currentIntervals.wlock();
    for (IJIntervalPtr interval : *currentIntervalLocked) {
        NES_DEBUG("interval {} is tested for triggering", interval->getId())
        switch (interval->intervalState) {
            case IJIntervalInfoState::LEFT_SIDE_FILLED:
                NES_DEBUG("interval state switches from LEFT_SIDE_FILLED TO ONCE_SEEN_DURING_TERMINATION ")
                interval->intervalState = IJIntervalInfoState::ONCE_SEEN_DURING_TERMINATION;
                break;
            case IJIntervalInfoState::EMITTED_TO_PROBE: break;
            case IJIntervalInfoState::ONCE_SEEN_DURING_TERMINATION: {
                NES_DEBUG("interval state switches from ONCE_SEEN_DURING_TERMINATION TO EMITTED_TO_PROBE")
                interval->intervalState = IJIntervalInfoState::EMITTED_TO_PROBE;
                // we can simply emit as interval is does not need to be combined with other intervals
                emitIntervalToProbe(interval, pipelineCtx);// that is tested and fine, happens only once
                break;
            }
            case IJIntervalInfoState::MARKED_FOR_DELETION: break;
        }
    }
    // afterwards, clean current interval vector from intervals with State 'MARKED_FOR_DELETION', i.e., remove the element using erase function and iterators
    for (auto it = currentIntervalLocked->begin(); it != currentIntervalLocked->end();) {
        if ((*it)->intervalState == IJIntervalInfoState::MARKED_FOR_DELETION) {
            NES_INFO("Removing interval {}", (*it)->toString());
            it = currentIntervalLocked->erase(it);
        } else {
            ++it;
        }
    }
}

void IJOperatorHandler::deleteAllIntervals() {
    auto currentIntervalsLocked = currentIntervals.wlock();
    currentIntervalsLocked->clear();
    auto rightTuplesLocked = rightTuples.wlock();
    rightTuplesLocked->clear();
}

void IJOperatorHandler::stop(QueryTerminationType queryTerminationType, PipelineExecutionContextPtr pipelineCtx) {
    NES_INFO("Stopped IJOperatorHandler with {}!", magic_enum::enum_name(queryTerminationType));
    if (queryTerminationType == QueryTerminationType::Graceful) {
        triggerAllIntervals(pipelineCtx.get());
    }
}

void IJOperatorHandler::setBufferManager(const NES::Runtime::BufferManagerPtr& bufManager) { this->bufferManager = bufManager; }
IJOperatorHandlerPtr IJOperatorHandler::create(const std::vector<OriginId>& inputOrigins,
                                               const OriginId outputOriginId,
                                               const int64_t lowerBound,
                                               const int64_t upperBound,
                                               const SchemaPtr& leftSchema,
                                               const SchemaPtr& rightSchema,
                                               const uint64_t pageSizeLeft,
                                               const uint64_t pageSizeRight) {
    return std::make_shared<IJOperatorHandler>(inputOrigins,
                                               outputOriginId,
                                               lowerBound,
                                               upperBound,
                                               leftSchema,
                                               rightSchema,
                                               pageSizeLeft,
                                               pageSizeRight);
}

void IJOperatorHandler::updateRightTuples(int64_t latestRightTupleBufferCleanTimeStamp) {
    auto rightTuplesLocked = rightTuples.wlock();
    rightTuplesLocked->clear();
    rightTuplesLocked->push_back(std::move(updatedRightTuples[0]));
    this->latestRightTupleBufferCleanTimeStamp = latestRightTupleBufferCleanTimeStamp;
    updatedRightTuples.clear();
}

std::vector<std::unique_ptr<Nautilus::Interface::PagedVectorVarSized>>& IJOperatorHandler::getUpdatedRightTuples() {
    return updatedRightTuples;
}

int64_t IJOperatorHandler::getSmallestIntervalStartSeen() { return smallestIntervalStartEmitted; }
int64_t IJOperatorHandler::getLatestRightTupleBufferCleanTimeStamp() { return latestRightTupleBufferCleanTimeStamp; }
BufferManagerPtr IJOperatorHandler::getBufferManager() { return bufferManager; }
uint64_t IJOperatorHandler::getPageSizeRight() const { return pageSizeRight; }
int64_t IJOperatorHandler::getUpperBound() const { return upperBound; };
int64_t IJOperatorHandler::getLowerBound() const { return lowerBound; }
OriginId IJOperatorHandler::getOutputOriginId() const { return outputOriginId; }
uint64_t IJOperatorHandler::getNextSequenceNumber() { return sequenceNumber++; }
std::shared_ptr<IJInterval> IJOperatorHandler::getInterval(uint64_t index) {
    auto currentIntervalsLocked = currentIntervals.wlock();
    return currentIntervalsLocked->at(index);
}
SchemaPtr& IJOperatorHandler::getRightSchema() { return rightSchema; }
}// namespace NES::Runtime::Execution::Operators
