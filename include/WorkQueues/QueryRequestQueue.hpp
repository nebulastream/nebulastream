#ifndef NES_QUERYREQUESTQUEUE_HPP
#define NES_QUERYREQUESTQUEUE_HPP

#include <API/QueryId.hpp>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <vector>

namespace NES {

class QueryCatalogEntry;
typedef std::shared_ptr<QueryCatalogEntry> QueryCatalogEntryPtr;

class QueryRequestQueue {

  public:
    QueryRequestQueue();

    /**
     * @brief Add query request into processing queue
     * @param queryCatalogEntry: the query request in form of query catalog entry
     * @return true if successfully added to the queue
     */
    bool add(QueryCatalogEntryPtr queryCatalogEntry);

    /**
     * @brief Get a batch of query catalog entries to be processed.
     * Note: This method returns only a copy of the
     * @return a vector of query catalog entry to schedule
     */
    std::vector<QueryCatalogEntry> getNextBatch();

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
    uint64_t batchSize;
    std::mutex queryRequest;
    std::condition_variable availabilityTrigger;
    std::deque<QueryCatalogEntry> schedulingQueue;
};
typedef std::shared_ptr<QueryRequestQueue> QueryRequestQueuePtr;
}// namespace NES
#endif//NES_QUERYREQUESTQUEUE_HPP
