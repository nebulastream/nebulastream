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
class NLJOperatorHandler : public OperatorHandler {

public:
    NLJOperatorHandler(size_t windowSize, const SchemaPtr &joinSchemaLeft, const SchemaPtr &joinSchemaRight,
                       const std::string &joinFieldNameLeft, const std::string &joinFieldNameRight,
                       const std::vector<OriginId>& origins);

    uint8_t* insertNewTuple(uint64_t timestamp, bool isLeftSide);

    void deleteWindow(uint64_t windowIdentifier);

    uint64_t getNumberOfTuplesInWindow(uint64_t windowIdentifier, bool isLeftSide);

    uint8_t* getFirstTuple(uint64_t windowIdentifier, bool isLeftSide);

    SchemaPtr getSchema(bool isLeftSide);

    std::pair<uint64_t, uint64_t> getWindowStartEnd(uint64_t windowIdentifier);

    void start(PipelineExecutionContextPtr pipelineExecutionContext, StateManagerPtr stateManager,
               uint32_t localStateVariableId) override;

    void stop(QueryTerminationType terminationType, PipelineExecutionContextPtr pipelineExecutionContext) override;

    std::vector<uint64_t> checkWindowsTrigger(uint64_t watermarkTs, uint64_t sequenceNumber, OriginId originId);

private:

    std::optional<NLJWindow*> getWindowByWindowIdentifier(uint64_t windowIdentifier);

    void createNewWindow(uint64_t timestamp);

    std::optional<NLJWindow*> getWindowByTimestamp(uint64_t timestamp);

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
