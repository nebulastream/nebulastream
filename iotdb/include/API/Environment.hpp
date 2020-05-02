#ifndef API_ENVIRONMENT_H
#define API_ENVIRONMENT_H


#include <API/Config.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <API/InputQuery.hpp>
#include <string>
#include "../SourceSink/DataSource.hpp"

namespace NES {

/** \brief the central abstraction for the user to define queries */
class Environment {
  public:

    /**
     * Initializes a execution environment according to the configuration.
     * @param config for the execution environment
     * @return
     */
    static Environment create(const Config& config);

    /**
     * Execute the query on environment.
     * @param inputQuery
     */
    void executeQuery(const InputQuery& inputQuery);

    /**
     * Prints a query plan for debugging.
     * @param inputQuery
     */
    void printInputQueryPlan(const InputQuery& inputQuery);


  private:
    Config config;
    Environment(const Config &config);
    QueryManagerPtr queryManager;
    BufferManagerPtr bufferManager;
    void printInputQueryPlan(const OperatorPtr curr, int depth);
};

} // namespace NES

#endif
