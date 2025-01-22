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
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

NLJOperatorHandlerSlicing::NLJOperatorHandlerSlicing(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const uint64_t windowSlide,
                                                     const SchemaPtr& leftSchema,
                                                     const SchemaPtr& rightSchema,
                                                     const uint64_t pageSizeLeft,
                                                     const uint64_t pageSizeRight,
                                                     TimeFunctionPtr leftTimeFunctionPtr,
                                                     TimeFunctionPtr rightTimeFunctionPtr,
                                                     std::map<QueryId, uint64_t> deploymentTimes)
    : StreamJoinOperatorHandler(inputOrigins,
                                outputOriginId,
                                windowSize,
                                windowSlide,
                                leftSchema,
                                rightSchema,
                                std::move(leftTimeFunctionPtr),
                                std::move(rightTimeFunctionPtr),
                                deploymentTimes),
      NLJOperatorHandler(pageSizeLeft, pageSizeRight) {}
NLJOperatorHandlerPtr NLJOperatorHandlerSlicing::create(const std::vector<OriginId>& inputOrigins,
                                                        const OriginId outputOriginId,
                                                        const uint64_t windowSize,
                                                        const uint64_t windowSlide,
                                                        const SchemaPtr& leftSchema,
                                                        const SchemaPtr& rightSchema,
                                                        const uint64_t pageSizeLeft,
                                                        const uint64_t pageSizeRight,
                                                        TimeFunctionPtr leftTimeFunctionPtr,
                                                        TimeFunctionPtr rightTimeFunctionPtr,
                                                        std::map<QueryId, uint64_t> deploymentTimes) {
    return std::make_shared<NLJOperatorHandlerSlicing>(inputOrigins,
                                                       outputOriginId,
                                                       windowSize,
                                                       windowSlide,
                                                       leftSchema,
                                                       rightSchema,
                                                       pageSizeLeft,
                                                       pageSizeRight,
                                                       std::move(leftTimeFunctionPtr),
                                                       std::move(rightTimeFunctionPtr),
                                                       deploymentTimes);
}

bool NLJOperatorHandlerSlicing::adjustSliceEndAndAddWindows(StreamSlicePtr slice,
                                                            uint64_t deploymentTime,
                                                            WLockedWindows& windowToSlicesLocked) {
    bool edgesChanged = false;
    if (slice->getSliceEnd() != sliceAssigner.getSliceEndTs(slice->getSliceStart())) {
        edgesChanged = true;
    }

    NES_INFO("slice might get adjusted. Old end time was {} and new end time is {}. Afterwards it will get added to all newly "
             "created windows",
             slice->getSliceEnd(),
             sliceAssigner.getSliceEndTs(slice->getSliceStart()))
    slice->setSliceEnd(sliceAssigner.getSliceEndTs(slice->getSliceStart()));

    for (auto windowInfo : getAllWindowsOfDeploymentTimeForSlice(*slice, deploymentTime)) {
        auto& window = (*windowToSlicesLocked)[windowInfo];

        window.slices.emplace_back(slice);
        NES_DEBUG("Added slice (sliceStart: {} sliceEnd: {} sliceId: {}) to window {}, #slices {}",
                  slice->getSliceStart(),
                  slice->getSliceEnd(),
                  slice->getSliceId(),
                  windowInfo.toString(),
                  window.slices.size())
    }
    return edgesChanged;
}

void NLJOperatorHandlerSlicing::removeRecordsWrongTsFromSlice(StreamSlicePtr slice,
                                                              WLockedSlices& slicesWriteLocked,
                                                              WLockedWindows& windowToSlicesLocked) {
    auto nljSlice = std::dynamic_pointer_cast<NLJSlice>(slice);
    NES_INFO("Potentially fixing tuples in NLJSlice with start {} and end {}", nljSlice->getSliceStart(), nljSlice->getSliceEnd())
    nljSlice->combinePagedVectors();

    //first we check all tuples on the left side of slices and then on the right side
    for (auto leftSide : {true, false}) {
        //setup memRef and schema
        auto nljPagedVectorMemRef = leftSide
            ? Value<MemRef>(static_cast<int8_t*>(nljSlice->getPagedVectorRefLeft(WorkerThreadId(0))))
            : Value<MemRef>(static_cast<int8_t*>(nljSlice->getPagedVectorRefRight(WorkerThreadId(0))));
        std::shared_ptr<Schema> schema = leftSide ? leftSchema : rightSchema;

        //get actual nautilus object for this
        Nautilus::Interface::PagedVectorVarSizedRef pagedVectorVarSizedRef(nljPagedVectorMemRef, schema);

        /*We iterate over each record and if we need to remove it we save the position of the record and the record itself.
            The position is given to the pagedVector to actually remove the record and the record itself is stored to then
            write it to the new correct slice that it now belongs to*/

        std::vector<Value<UInt64>> positionsToRemove;
        std::vector<Record> removedRecords;
        // we go from the back to the front, as there might be optimizations later on that we don't have to look at records in
        // the front, if the ts difference is big enough. For posRecord equal to '-1' we have an overflow and the loop stops.
        auto posLastRecord = pagedVectorVarSizedRef.getTotalNumberOfEntries();
        for (auto posRecord = posLastRecord - 1_u64; posRecord < posLastRecord; posRecord = posRecord - 1_u64) {
            auto record = pagedVectorVarSizedRef.readRecord(posRecord.as<UInt64>());
            auto ts =
                leftSide ? leftTimeFunctionPtr->getTsWithoutContext(record) : rightTimeFunctionPtr->getTsWithoutContext(record);

            // If ts outside of slice time (defined by t_start to t_end) store the record position and the record itself
            if ((ts->getValue() < nljSlice->getSliceStart() || ts->getValue() >= nljSlice->getSliceEnd())) {
                NES_DEBUG("removing Record {} with ts {} from its old slice and writing it to new slice. Its on the left? {}",
                          record.toString(),
                          ts->getValue(),
                          leftSide)

                positionsToRemove.push_back(posRecord.as<UInt64>());
                removedRecords.push_back(record);
            }
        }

        // Actually remove false records from this page // Set true or false for internal approach of the method
        pagedVectorVarSizedRef.removeRecordsAndAdjustPositions(positionsToRemove, true);

        // For each removed record get or create the slice for the ts of this record and write the record into this slice
        for (auto record : removedRecords) {
            auto ts =
                leftSide ? leftTimeFunctionPtr->getTsWithoutContext(record) : rightTimeFunctionPtr->getTsWithoutContext(record);
            auto actualSliceForRecordPtr =
                getSliceByTimestampOrCreateItLocked(ts->getValue(), slicesWriteLocked, windowToSlicesLocked);
            auto actualSliceForRecord = std::dynamic_pointer_cast<NLJSlice>(actualSliceForRecordPtr);
            auto actualSlicePagedVector = leftSide
                ? Value<MemRef>(static_cast<int8_t*>(actualSliceForRecord->getPagedVectorRefLeft(WorkerThreadId(0))))
                : Value<MemRef>(static_cast<int8_t*>(actualSliceForRecord->getPagedVectorRefRight(WorkerThreadId(0))));
            Nautilus::Interface::PagedVectorVarSizedRef actualSlicePagedVectorRef(actualSlicePagedVector, schema);
            actualSlicePagedVectorRef.writeRecord(record);
        }
    }
}

void NLJOperatorHandlerSlicing::addQueryToSharedJoinApproachDeleting(QueryId queryId, uint64_t deploymentTime) {
    if (getApproach() == SharedJoinApproach::UNSHARED || getApproach() == SharedJoinApproach::APPROACH_DELETING) {
        setApproach(SharedJoinApproach::APPROACH_DELETING);
    } else {
        NES_ERROR("approach to adding a query to a shared join should not be changed during a test")
    }

    deploymentTimes.emplace(queryId, deploymentTime);
    sliceAssigner.addWindowDeploymentTime(deploymentTime);

    NES_INFO("Add new query that uses same joinOperatorHandler with approach 2 (deleting). QueryId {}, deployment time {}, "
             "number of queries handler is handling now {}",
             queryId,
             deploymentTime,
             deploymentTimes.size())

    auto [slicesWriteLocked, windowToSlicesLocked] = folly::acquireLocked(slices, windowToSlices);

    auto originalSize = slicesWriteLocked->size();
    auto sliceIterator = slicesWriteLocked->begin();
    auto count = 0_u64;

    while (count < originalSize) {

        if ((*sliceIterator)->getSliceEnd() < deploymentTime) {
            NES_INFO("slice not affected by new query deployment, because slice end is {}, (start is {})",
                     (*sliceIterator)->getSliceEnd(),
                     (*sliceIterator)->getSliceStart())
            ++sliceIterator;
            ++count;
            continue;
        }

        auto edgesChanged = adjustSliceEndAndAddWindows((*sliceIterator), deploymentTime, windowToSlicesLocked);

        if (edgesChanged) {
            removeRecordsWrongTsFromSlice((*sliceIterator), slicesWriteLocked, windowToSlicesLocked);
        }

        ++sliceIterator;
        ++count;
    }
}

void NLJOperatorHandlerSlicing::checkAndLogTimestamp(uint64_t joinBuildSideInt) {
    NES_DEBUG("got buffer {}", numberOfRecreatedBuffers[joinBuildSideInt].load());
    if (numberOfRecreatedBuffers[joinBuildSideInt].fetch_add(1, std::memory_order_relaxed) == numberOfBuffersToRecreate - 1) {
        auto endTime = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        NES_ERROR("thread finished recreating at {}", endTime);
    }
}
}// namespace NES::Runtime::Execution::Operators
