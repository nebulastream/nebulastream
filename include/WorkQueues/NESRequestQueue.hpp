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

#ifndef NES_NESREQUESTQUEUE_HPP
#define NES_NESREQUESTQUEUE_HPP

#include <Plans/Query/QueryId.hpp>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

namespace NES {

class NESRequest;
typedef std::shared_ptr<NESRequest> NESRequestPtr;

/**
 * @brief This is a wrapper around a Deque for submitting arbitrary requests for the RequestProcessorService
 */
class NESRequestQueue {

  public:
    /**
     * @brief Constructor of Query request queue
     * @param batchSize : the batch of user requests to be processed together
     */
    NESRequestQueue(uint32_t batchSize);

    //d-tor
    ~NESRequestQueue();

    /**
     * @brief Add query request into processing queue
     * @param request: the query request
     * @return true if successfully added to the queue
     */
    bool add(NESRequestPtr request);

    /**
     * @brief Get a batch of query catalog entries to be processed.
     * Note: This method returns only a copy of the
     * @return a vector of query catalog entry to schedule
     */
    std::vector<NESRequestPtr> getNextBatch();

    /**
     * @brief Check if there are new request available
     * @return true if there are new requests
     */
    bool isNewRequestAvailable() const;

    /**
     * @brief Change status of new request availability
     * @param newRequestAvailable: bool indicating if the request is available
     */
    void setNewRequestAvailable(bool newRequestAvailable);

    /**
     * @brief This method will force trigger the availabilityTrigger conditional variable in order to
     * interrupt the wait.
     */
    void insertPoisonPill();

  private:
    bool newRequestAvailable;
    uint32_t batchSize;
    std::mutex requestMtx;
    std::condition_variable availabilityTrigger;
    std::deque<NESRequestPtr> requestQueue;
};
typedef std::shared_ptr<NESRequestQueue> NESRequestQueuePtr;
}// namespace NES
#endif//NES_NESREQUESTQUEUE_HPP
