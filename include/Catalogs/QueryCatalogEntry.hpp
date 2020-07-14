#ifndef NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_
#define NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_

#include <Topology/NESTopologyEntry.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class TopologyManager;
typedef std::shared_ptr<TopologyManager> TopologyManagerPtr;

class NESExecutionPlan;
typedef std::shared_ptr<NESExecutionPlan> NESExecutionPlanPtr;

/**
 * @brief Represents various states the user query goes through.
 *
 * Registered : Query is registered to be scheduled to the worker nodes
 * Scheduling: Coordinator node is processing the Query and will transmit the execution pipelines to worker nodes
 * Running: Query is now running successfully
 * Stopped: Query was explicitly stopped by system
 * Failed: Query failed because of some reason
 *
 */
enum QueryStatus { Registered,
                   Scheduling,
                   Running,
                   MarkedForStop,
                   Stopped,
                   Failed };

static std::map<std::string, QueryStatus> stringToQueryStatusMap{
    {"REGISTERED", Registered},
    {"SCHEDULING", Scheduling},
    {"RUNNING", Running},
    {"MARKED_FOR_STOP", MarkedForStop},
    {"STOPPED", Stopped},
    {"FAILED", Failed},
};

static std::map<QueryStatus, std::string> queryStatusToStringMap{
    {Registered, "REGISTERED"},
    {Scheduling, "SCHEDULING"},
    {Running, "RUNNING"},
    {MarkedForStop, "MARKED_FOR_STOP"},
    {Stopped, "STOPPED"},
    {Failed, "FAILED"},
};

/**
 * @brief class to handle the entry in the query catalog
 * @param queryId: id of the query (is also the key in the queries map)
 * @param queryString: string representation of the query
 * @param QueryPtr: a pointer to the query
 * @param schema: the schema of this query
 * @param running: bool indicating if the query is running (has been deployed)
 */
class QueryCatalogEntry {
  public:
    QueryCatalogEntry(std::string queryId, std::string queryString, QueryPlanPtr queryPlanPtr, QueryStatus queryStatus)
        : queryId(queryId),
          queryString(queryString),
          queryPlanPtr(queryPlanPtr),
          queryStatus(queryStatus) {
    }

    /**
     * @brief method to get the id of the query
     * @return query id
     */
    std::string getQueryId(){
        return queryId;
    }

    /**
     * @brief method to get the string of the query
     * @return query string
     */
    std::string getQueryString() {
        return queryString;
    }

    /**
     * @brief method to get the query plan
     * @return pointer to the query plan
     */
    const QueryPlanPtr getQueryPlan() const {
        return queryPlanPtr;
    }

    /**
     * @brief method to get the status of the query
     * @return query status
     */
    QueryStatus getQueryStatus() const {
        return queryStatus;
    }

    /**
     * @brief method to get the status of the query as string
     * @return query status: as string
     */
    std::string getQueryStatusAsString() const {
        return queryStatusToStringMap[queryStatus];
    }

    /**
     * @brief method to set the status of the query
     * @param query status
     */
    void setQueryStatus(QueryStatus queryStatus) {
        QueryCatalogEntry::queryStatus = queryStatus;
    }

    /**
      * @brief Get name of the query placement strategy
      * @return query placement strategy
      */
    const std::string& getQueryPlacementStrategy() const {
        return queryPlacementStrategy;
    }

  private:
    std::string queryId;
    std::string queryString;
    std::string queryPlacementStrategy;
    QueryPlanPtr queryPlanPtr;
    QueryStatus queryStatus;
};

typedef std::shared_ptr<QueryCatalogEntry> QueryCatalogEntryPtr;

}// namespace NES

#endif//NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_
