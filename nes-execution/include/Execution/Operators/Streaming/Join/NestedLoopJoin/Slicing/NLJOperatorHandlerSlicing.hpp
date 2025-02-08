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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_SLICING_NLJOPERATORHANDLERSLICING_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_SLICING_NLJOPERATORHANDLERSLICING_HPP_
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerSlicing.hpp>
#include <fstream>

namespace NES::Runtime::Execution::Operators {

class NLJOperatorHandlerSlicing : public NLJOperatorHandler, public StreamJoinOperatorHandlerSlicing {
  public:
    /**
     * @brief Constructor for a NLJOperatorHandlerSlicing
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param pageSizeLeft
     * @param pageSizeRight
     */
    NLJOperatorHandlerSlicing(const std::vector<OriginId>& inputOrigins,
                              const OriginId outputOriginId,
                              const uint64_t windowSize,
                              const uint64_t windowSlide,
                              const SchemaPtr& leftSchema,
                              const SchemaPtr& rightSchema,
                              const uint64_t pageSizeLeft,
                              const uint64_t pageSizeRight,
                              TimeFunctionPtr leftTimeFunctionPtr,
                              TimeFunctionPtr rightTimeFunctionPtr,
                              std::map<QueryId, uint64_t> deploymentTimes);

    /**
     * @brief Creates a NLJOperatorHandlerSlicing
     * @param inputOrigins
     * @param outputOriginId
     * @param windowSize
     * @param windowSlide
     * @param sizeOfRecordLeft
     * @param sizeOfRecordRight
     * @param pageSizeLeft
     * @param pageSizeRight
     * @return NLJOperatorHandlerPtr
     */
    static NLJOperatorHandlerPtr create(const std::vector<OriginId>& inputOrigins,
                                        const OriginId outputOriginId,
                                        const uint64_t windowSize,
                                        const uint64_t windowSlide,
                                        const SchemaPtr& leftSchema,
                                        const SchemaPtr& rightSchema,
                                        const uint64_t pageSizeLeft,
                                        const uint64_t pageSizeRight,
                                        TimeFunctionPtr leftTimeFunctionPtr,
                                        TimeFunctionPtr rightTimeFunctionPtr,
                                        std::map<QueryId, uint64_t> deploymentTimes);

    void addQueryToSharedJoinApproachDeleting(QueryId queryId, uint64_t deploymentTime) override;

    void checkAndLogTimestamp(uint64_t joinBuildSideInt);

    uint64_t numberOfBuffersToRecreate = -1;
    std::array<std::atomic<uint64_t>, 2> numberOfRecreatedBuffers{0, 0};

    ~NLJOperatorHandlerSlicing() override = default;

  private:
    /// The output file path of the file sink.
//    std::string filePath = "/local-ssd/ankit/sr630-wn-a-10-nes-apr/recreation_file_node_0_completed.bin";
    std::string filePath = "recreation_file_node_0.bin";

    /// The output file stream.
    std::ofstream outputFile;
    /**
     * Calculates the slice end of this slice which might have changed since this join is now shared by an additional query deployed
     * at a different time. Regardless if the end changed the slice then gets added to each window that was added by this new query.
     * @param slice
     * @param windowToSlicesLocked
     * @return true iff the end time of the slice has changed
     */
    bool adjustSliceEndAndAddWindows(StreamSlicePtr slice, uint64_t deploymentTime, WLockedWindows& windowToSlicesLocked);

    /**
     * This method iterates over all records inside a slice and compares their timestamp against the end time of the slice. Each
     * record was added before the slices end time changed. In case it does not belong to the slice anymore the record is physically
     * removed from the pagedVectorVarSizedRef (if the record contains varSized fields there are different policies for these fields).
     * Each removed record is then put into the correct slice, which might be created first for this reason.
     * @note should to be called after adjusting slice end times.
     * @param nljSlice the slice in question
     * @param slicesWriteLocked all slices write locked
     * @param windowToSlicesLocked all windows write locked
     */
    void removeRecordsWrongTsFromSlice(StreamSlicePtr nljSlice,
                                       WLockedSlices& slicesWriteLocked,
                                       WLockedWindows& windowToSlicesLocked);
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_SLICING_NLJOPERATORHANDLERSLICING_HPP_
