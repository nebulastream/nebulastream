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

namespace NES::Runtime::Execution::Operators {
class NLJOperatorHandler : public OperatorHandler {

public:

    uint8_t* insertNewTuple(uint64_t timestamp, bool isLeftSide);

    void createNewWindow(bool isLeftSide);

    void deleteWindow(uint64_t timestamp);

    void incrementLastTupleTimestamp(bool isLeftSide);

    bool updateStateOfNLJWindows(uint64_t timestamp, bool isLeftSide);

    std::list<NLJWindow>& getAllNLJWindows();

private:
    NLJWindow& getWindow(uint64_t timestamp);


    std::list<NLJWindow> nljWindows;
    size_t windowSize;
    uint64_t counterFinishedBuildingStart;
    SchemaPtr joinSchemaLeft;
    SchemaPtr joinSchemaRight;
};
} // namespace NES::Runtime::Execution::Operators

#endif //NES_NLJOPERATORHANDLER_HPP
