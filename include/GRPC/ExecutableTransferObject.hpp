#ifndef INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_
#define INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_

#include <API/Schema.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <SourceSink/DataSink.hpp>
#include <SourceSink/DataSource.hpp>

#include <string>
#include <vector>

#include <QueryCompiler/QueryCompiler.hpp>

using std::string;
using std::vector;

namespace NES {

class ExecutableTransferObject {
  public:
    ExecutableTransferObject() = default;
    ExecutableTransferObject(string queryId,
                             SchemaPtr schema,
                             vector<DataSourcePtr> sources,
                             vector<DataSinkPtr> destinations,
                             OperatorPtr operatorTree);
    ~ExecutableTransferObject();

  public:
    string& getQueryId();
    void setQueryId(const string& queryId);
    SchemaPtr getSchema();
    void setSchema(SchemaPtr schema);
    vector<DataSourcePtr>& getSources();
    void setSources(const vector<DataSourcePtr>& sources);
    vector<DataSinkPtr>& getDestinations();
    void setDestinations(const vector<DataSinkPtr>& destinations);
    OperatorPtr& getOperatorTree();
    void setOperatorTree(const OperatorPtr& operatorTree);

    QueryExecutionPlanPtr toQueryExecutionPlan(QueryCompilerPtr queryCompiler);

    std::string toString() const;

  private:
    string queryId;
    SchemaPtr schema;
    vector<DataSourcePtr> sources;
    vector<DataSinkPtr> destinations;
    OperatorPtr operatorTree;
    bool compiled = false;
};

}// namespace NES

#endif//INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_
