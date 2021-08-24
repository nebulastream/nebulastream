/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <Util/Logger.hpp>
#include <WorkQueues/NESRequestQueue.hpp>
#include <WorkQueues/RequestTypes/NESRequest.hpp>
#include <algorithm>

namespace NES {

NESRequestQueue::NESRequestQueue(uint64_t batchSize) : newRequestAvailable(false), batchSize(batchSize) {
    NES_DEBUG("QueryRequestQueue()");
}

NESRequestQueue::~NESRequestQueue() { NES_DEBUG("~QueryRequestQueue()"); }

bool NESRequestQueue::add(const NESRequestPtr& request) {
    std::unique_lock<std::mutex> lock(requestMutex);
    NES_INFO("QueryRequestQueue: Adding a new query request : " << request->toString());
    //TODO: identify and handle if more than one request for same query exists in the queue
    requestQueue.emplace_back(request);
    NES_INFO("QueryCatalog: Marking that new request is available to be scheduled");
    setNewRequestAvailable(true);
    availabilityTrigger.notify_one();
    return true;
}

std::vector<NESRequestPtr> NESRequestQueue::getNextBatch() {
    std::unique_lock<std::mutex> lock(requestMutex);
    //We are using conditional variable to prevent Lost Wakeup and Spurious Wakeup
    //ref: https://www.modernescpp.com/index.php/c-core-guidelines-be-aware-of-the-traps-of-condition-variables
    availabilityTrigger.wait(lock, [&] {
        return isNewRequestAvailable();
    });
    NES_INFO("QueryRequestQueue: Fetching Queries to Schedule");
    std::vector<NESRequestPtr> queriesToSchedule;
    queriesToSchedule.reserve(batchSize);
    if (!requestQueue.empty()) {
        uint64_t currentBatchSize = 1;
        //Prepare a batch of queries to schedule
        while (currentBatchSize <= batchSize && !requestQueue.empty()) {
            queriesToSchedule.emplace_back(requestQueue.front());
            requestQueue.pop_front();
            currentBatchSize++;
        }
        NES_INFO("QueryRequestQueue: Scheduling " << queriesToSchedule.size() << " queries.");
        setNewRequestAvailable(!requestQueue.empty());
        return queriesToSchedule;
    }
    NES_INFO("QueryRequestQueue: Nothing to schedule.");
    setNewRequestAvailable(!requestQueue.empty());
    return queriesToSchedule;
}

void NESRequestQueue::insertPoisonPill() {
    std::unique_lock<std::mutex> lock(requestMutex);
    NES_INFO("QueryRequestQueue: Shutdown is called. Inserting Poison pill in the query request queue.");
    setNewRequestAvailable(true);
    availabilityTrigger.notify_one();
}

bool NESRequestQueue::isNewRequestAvailable() const { return newRequestAvailable; }

void NESRequestQueue::setNewRequestAvailable(bool newRequestAvailable) { this->newRequestAvailable = newRequestAvailable; }

}// namespace NES