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

static std::map<std::string, QueryStatus> stringToQueryStatusMap{
    {"REGISTERED", Registered},
    {"SCHEDULING", Scheduling},
    {"RUNNING", Running},
    {"STOPPED", Stopped},
    {"FAILED", Failed},
};

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

    const string& getQueryId() const {
        return queryId;
    }

    const string& getQueryString() const {
        return queryString;
    }

    const QueryPlanPtr getQueryPlan() const {
        return queryPlanPtr;
    }

    const NESExecutionPlanPtr getNesPlanPtr() const {
        return nesPlanPtr;
    }

    QueryStatus getQueryStatus() const {
        return queryStatus;
    }
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
