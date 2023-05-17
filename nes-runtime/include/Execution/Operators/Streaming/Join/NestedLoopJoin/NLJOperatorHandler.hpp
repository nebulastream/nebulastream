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
#ifndef NES_NLJOPERATORHANDLER_HPP
#define NES_NLJOPERATORHANDLER_HPP

#include <API/Schema.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
#include <list>
#include <Execution/Operators/Streaming/SliceAssigner.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>

namespace NES::Runtime::Execution::Operators {
/**
 * @brief This operator handler stores multiple windows (NLJWindow) with each window containing the left and right stream tuples.
 * This class provides the two join phases (NLJBuild and NLJSink) with methods for performing a nested loop join.
 */
class NLJOperatorHandler : public OperatorHandler {
public:
    /**
     * @brief Constructor for a NLJOperatorHandler
     * @param windowSize
     * @param joinSchemaLeft
     * @param joinSchemaRight
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param origins
     */
    NLJOperatorHandler(size_t windowSize, const SchemaPtr &joinSchemaLeft, const SchemaPtr &joinSchemaRight,
                       const std::string &joinFieldNameLeft, const std::string &joinFieldNameRight,
                       const std::vector<OriginId>& origins);

    /**
     * @brief Creates an entry for a new tuple by first getting the responsible window for the timestamp. This method is thread safe.
     * @param timestamp
     * @param isLeftSide
     * @return Pointer
     */
    uint8_t* insertNewTuple(uint64_t timestamp, bool isLeftSide);

    /**
     * @brief Deletes a window
     * @param windowIdentifier
     */
    void deleteWindow(uint64_t windowIdentifier);

    /**
     * @brief Retrieves the number of tuples for a stream (left or right) and a window
     * @param windowIdentifier
     * @param isLeftSide
     * @return Number of tuples or -1 if no window exists for the window identifier
     */
    uint64_t getNumberOfTuplesInWindow(uint64_t windowIdentifier, bool isLeftSide);

    /**
     * @brief Retrieves the pointer to the first tuple for a stream (left or right) and a window
     * @param windowIdentifier
     * @param isLeftSide
     * @return Pointer
     */
    uint8_t* getFirstTuple(uint64_t windowIdentifier, bool isLeftSide);

    /**
     * @brief Retrieves the schema for the left or right stream
     * @param isLeftSide
     * @return SchemaPtr
     */
    SchemaPtr getSchema(bool isLeftSide);

    /**
     * @brief Retrieves the start and end of a window
     * @param windowIdentifier
     * @return Pair<windowStart, windowEnd>
     */
    std::pair<uint64_t, uint64_t> getWindowStartEnd(uint64_t windowIdentifier);

    /**
     * @brief Starts the operator handler
     * @param pipelineExecutionContext
     * @param stateManager
     * @param localStateVariableId
     */
    void start(PipelineExecutionContextPtr pipelineExecutionContext, StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    /**
     * @brief Stops the operator handler
     * @param terminationType
     * @param pipelineExecutionContext
     */
    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    /**
     * @brief Checks if any window can be triggered and all triggerable window identifiers are being returned in a vector.
     * This method updates the watermarkProcessor and should be thread-safe
     * @param watermarkTs
     * @param sequenceNumber
     * @param originId
     * @return Vector<uint64_t> containing windows that can be triggered
     */
    std::vector<uint64_t> checkWindowsTrigger(uint64_t watermarkTs, uint64_t sequenceNumber, OriginId originId);

private:
    /**
     * @brief Retrieves the window by a window identifier. If no window exists for the windowIdentifier, the optional has no value.
     * @param windowIdentifier
     * @return optional
     */
    std::optional<NLJWindow*> getWindowByWindowIdentifier(uint64_t windowIdentifier);

    /**
     * @brief Retrieves the window by a window timestamp. If no window exists for the timestamp, the optional has no value.
     * @param timestamp
     * @return
     */
    std::optional<NLJWindow*> getWindowByTimestamp(uint64_t timestamp);

    /**
     * @brief Creates a new window that corresponds to this timestamp
     * @param timestamp
     */
    void createNewWindow(uint64_t timestamp);

    std::mutex insertNewTupleMutex;
    std::list<NLJWindow> nljWindows;
    SliceAssigner sliceAssigner;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    SchemaPtr joinSchemaLeft;
    SchemaPtr joinSchemaRight;
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
};
} // namespace NES::Runtime::Execution::Operators

#endif //NES_NLJOPERATORHANDLER_HPP
