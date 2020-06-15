#ifndef NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_
#define NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_

#include <Topology/NESTopologyEntry.hpp>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace std;

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
 * Scheduling: Coordinator node is transmitting the execution pipelines to worker nodes
 * Running: Query is now running successfully
 * Stopped: Query was explicitly stopped by system
 * Failed: Query failed because of some reason
 *
 */
enum QueryStatus { Registered,
                   Scheduling,
                   Running,
                   Stopped,
                   Failed };

/**
 * @brief class to handle the entry in the query catalog
 * @param queryId: id of the query (is also the key in the queries map)
 * @param queryString: string representation of the query
 * @param QueryPtr: a pointer to the query
 * @param nesPlanPtr: a pointer to the generated nes execution plan
 * @param schema: the schema of this query
 * @param running: bool indicating if the query is running (has been deployed)
 */
class QueryCatalogEntry {
  public:
    QueryCatalogEntry(string queryId, string queryString, QueryPlanPtr queryPlanPtr,
                      NESExecutionPlanPtr nesPlanPtr, QueryStatus queryStatus)
        : queryId(queryId),
          queryString(queryString),
          queryPlanPtr(queryPlanPtr),
          nesPlanPtr(nesPlanPtr),
          queryStatus(queryStatus) {
    }

    /**
    * @brief method to get the id of the query
    * @return query id
    */
    const string& getQueryId() const {
        return queryId;
    }

    /**
    * @brief method to get the string of the query
    * @return query string
    */
    const string& getQueryString() const {
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
    * @brief method to get the execution plan of the query
    * @return pointer to the nes execution plan
    */
    const NESExecutionPlanPtr getNesPlanPtr() const {
        return nesPlanPtr;
    }

    /**
    * @brief method to get the status of the query
    * @return query status
    */
    QueryStatus getQueryStatus() const {
        return queryStatus;
    }

    /**
    * @brief method to set the status of the query
    * @param query status
    */
    void setQueryStatus(QueryStatus queryStatus) {
        QueryCatalogEntry::queryStatus = queryStatus;
    }

  private:
    string queryId;
    string queryString;
    QueryPlanPtr queryPlanPtr;
    NESExecutionPlanPtr nesPlanPtr;
    QueryStatus queryStatus;
};

typedef std::shared_ptr<QueryCatalogEntry> QueryCatalogEntryPtr;

}// namespace NES

#endif//NES_INCLUDE_CATALOGS_QUERYCATALOGENTRY_HPP_
