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
#include <Nautilus/Tracing/Tag/TagRecorder.hpp>
#include <Nautilus/Tracing/TraceContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
namespace NES::Nautilus::Tracing {

bool SymbolicExecutionContext::record(TagRecorder& tr) {
    // special case if we are currently in the follow mode, we switch to record and change the last decision in this execution path.
    if (currentMode == SymbolicExecutionContext::MODE::FOLLOW) {
        currentMode = SymbolicExecutionContext::MODE::RECORD;
        currentExecutionPath.getPath().pop_back();
    }

    auto tag = tr.createTag();
    auto foundTag = tagMap.find(tag);
    if (foundTag == tagMap.end()) {
        // If was not visited yet -> store the execution trace and return true.
        tagMap.emplace(tag, SymbolicExecutionContext::TagState::FirstVisit);
        currentExecutionPath.append(true);
        currentExecutionPath.setFinalTag(tag);
        inflightExecutionPaths.emplace_back(currentExecutionPath);
        return true;
    }
    // The tag already exists in the tag map.
    // Thus, the if was visited at least once.
    switch (foundTag->second) {
        case SymbolicExecutionContext::TagState::FirstVisit: {
            // Tag is in FirstVisit state. Thus, it was visited one time -> so we visit the false case.
            foundTag->second = SymbolicExecutionContext::TagState::SecondVisit;
            currentExecutionPath.append(false);
            return false;
        };
        case SymbolicExecutionContext::TagState::SecondVisit: {
            // The tag is in SecondVisit state -> terminate execution.
            NES_DEBUG("Trace: early terminate via exception.");
            TraceContext::get()->pause();
            // todo disable tracing
            throw TraceTerminationException();
        };
    }
}

bool SymbolicExecutionContext::executeCMP(TagRecorder& tr) {
    currentOperation++;
    if (currentMode == SymbolicExecutionContext::MODE::FOLLOW && currentOperation < currentExecutionPath.getSize()) {
        auto operation = currentExecutionPath[currentOperation - 1];
        return get<0>(operation);
    } else {
        return record(tr);
    }
}

bool SymbolicExecutionContext::shouldContinue() {
    if (iterations == 0) {
        return true;
    }
    while (!inflightExecutionPaths.empty()) {
        auto& trace = inflightExecutionPaths.front();
        auto& nextTracePath = trace.getPath();
        if (nextTracePath.empty()) {
            return true;
        }
        auto element = tagMap.find(trace.getFinalTag());
        NES_ASSERT(element != tagMap.end(), "the tag should exists at this point");
        if (element->second == SymbolicExecutionContext::TagState::FirstVisit) {
            return true;
        } else if (element->second == SymbolicExecutionContext::TagState::SecondVisit) {
            // the target tag of this path was already visited two times, so tracing can skip it.
            std::stringstream first;
            first << element->first;
            NES_DEBUG("Skip tag {}", first.str());
            inflightExecutionPaths.pop_front();
        }
    };
    return false;
}
void SymbolicExecutionContext::next() {
    if (iterations >= MAX_ITERATIONS) {
        NES_THROW_RUNTIME_ERROR("Tracing got lost and reached the max number of iterations.");
    }
    // if this is the first iteration the execution context is already initialized
    if (iterations > 0) {
        auto& trace = inflightExecutionPaths.front();
        currentMode = SymbolicExecutionContext::MODE::FOLLOW;
        currentExecutionPath = std::move(trace);
        inflightExecutionPaths.pop_front();
        currentOperation = 0;
    }
    iterations++;
}
uint64_t SymbolicExecutionContext::getIterations() const { return iterations; }

}// namespace NES::Nautilus::Tracing