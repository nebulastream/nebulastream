#ifndef INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_
#define INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_

#include <API/Schema.hpp>
#include <SourceSink/DataSource.hpp>
#include <SourceSink/DataSink.hpp>
#include <Operators/Operator.hpp>
#include <QueryCompiler/QueryExecutionPlan.hpp>
#include <QueryCompiler/CodeGenerator.hpp>
#include <QueryCompiler/GeneratedQueryExecutionPlan.hpp>

#include <string>
#include <vector>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <QueryCompiler/QueryCompiler.hpp>

using std::string;
using std::vector;

namespace NES {

class ExecutableTransferObject {
 public:
  ExecutableTransferObject() = default;
  ExecutableTransferObject(string queryId,
                           const Schema& schema,
                           vector<DataSourcePtr> sources,
                           vector<DataSinkPtr> destinations,
                           OperatorPtr operatorTree);
  ~ExecutableTransferObject() = default;

 public:
  string &getQueryId();
  void setQueryId(const string &queryId);
  Schema &getSchema();
  void setSchema(const Schema &schema);
  vector<DataSourcePtr> &getSources();
  void setSources(const vector<DataSourcePtr> &sources);
  vector<DataSinkPtr> &getDestinations();
  void setDestinations(const vector<DataSinkPtr> &destinations);
  OperatorPtr &getOperatorTree();
  void setOperatorTree(const OperatorPtr &operatorTree);

  QueryExecutionPlanPtr toQueryExecutionPlan(QueryCompilerPtr queryCompiler);

  std::string toString() const;

 private:
  string queryId;
  Schema schema;
  vector<DataSourcePtr> sources;
  vector<DataSinkPtr> destinations;
  OperatorPtr operatorTree;
  bool compiled = false;

  /**
   * @brief method for serialization, all listed variable below are added to the
   * serialization/deserialization process
   */
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar,
                 const unsigned int version) {
    ar & queryId;
    ar & schema;
    ar & sources;
    ar & destinations;
    ar & operatorTree;
    ar & compiled;
  }
};

}

#endif //INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_
