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
#include <Nautilus/IR/Types/IntegerStamp.hpp>
#include <Nautilus/Tracing/SymbolicExecution/SymbolicExecutionContext.hpp>
#include <Nautilus/Tracing/SymbolicExecution/TraceTerminationException.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
namespace NES::Nautilus::Tracing {

SymbolicExecutionContext::SymbolicExecutionContext() {}

bool SymbolicExecutionContext::follow(TagRecorder*) {
    currentOperation++;
    auto operation = currentExecutionPath[currentOperation - 1];
    return get<0>(operation);
}

bool SymbolicExecutionContext::record(TagRecorder* tr) {
    auto tag = tr->createTag();
    auto foundTag = tagMap.find(tag);
    if (foundTag == tagMap.end()) {
        // If was not visited yet -> store the execution trace and return true.
        tagMap.emplace(tag, FirstVisit);
        inflightExecutionPaths.emplace_back(currentExecutionPath);
        currentExecutionPath.append(true, tag);
        currentOperation++;
        return true;
    }
    // The tag already exists in the tag map.
    // Thus, the if was visited at least once.
    switch (foundTag->second) {
        case FirstVisit: {
            // Tag is in FirstVisit state. Thus, it was visited one time -> so we visit the false case.
            foundTag->second = SecondVisit;
            currentExecutionPath.append(false, tag);
            currentOperation++;
            return false;
        };
        case SecondVisit: {
            // The tag is in SecondVisit state -> terminate execution.
            throw TraceTerminationException();
        };
    }
}

bool SymbolicExecutionContext::executeCMP(TagRecorder* tr) {
    bool result = true;
    switch (currentMode) {
        case FOLLOW: {
            // check if current operation is the last operation -> transfer to record.
            if (currentExecutionPath.getPath().empty() || currentOperation == currentExecutionPath.getSize()) {
                currentMode = RECORD;
                return record(tr);
            } else {
                return follow(tr);
            }
        };
        case RECORD: {
            return record(tr);
        };
    };
}

bool SymbolicExecutionContext::shouldContinue() { return iterations == 0 || !inflightExecutionPaths.empty(); }
void SymbolicExecutionContext::next() {
    // if this is the first iteration the execution context is already initialized
    if (iterations > 0) {
        auto& trace = inflightExecutionPaths.front();
        currentMode = FOLLOW;
        currentExecutionPath = std::move(trace);
        inflightExecutionPaths.pop_front();
        currentOperation = 0;
    }
    iterations++;
}

}// namespace NES::Nautilus::Tracing