//
// Created by xchatziliadis on 19.11.19.
//

#ifndef IOTDB_INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_
#define IOTDB_INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_

#include <API/Schema.hpp>
#include <SourceSink/DataSource.hpp>
#include <SourceSink/DataSink.hpp>
#include <Operators/Operator.hpp>
#include <CodeGen/QueryExecutionPlan.hpp>
#include <CodeGen/CodeGen.hpp>
#include <CodeGen/GeneratedQueryExecutionPlan.hpp>

#include <string>
#include <vector>

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

using std::string;
using std::vector;

namespace iotdb {

class ExecutableTransferObject {
 public:
  ExecutableTransferObject() = default;
  ExecutableTransferObject(string description,
                           Schema schema,
                           vector<DataSourcePtr> sources,
                           vector<DataSinkPtr> destinations,
                           OperatorPtr operatorTree);
  ~ExecutableTransferObject() = default;

 public:
  const string &getDescription() const;
  void setDescription(const string &description);
  const Schema &getSchema() const;
  void setSchema(const Schema &schema);
  const vector<DataSourcePtr> &getSources() const;
  void setSources(const vector<DataSourcePtr> &sources);
  const vector<DataSinkPtr> &getDestinations() const;
  void setDestinations(const vector<DataSinkPtr> &destinations);
  const OperatorPtr &getOperatorTree() const;
  void setOperatorTree(const OperatorPtr &operatorTree);

  QueryExecutionPlanPtr toQueryExecutionPlan();

 private:
  string _description;
  Schema _schema;
  vector<DataSourcePtr> _sources;
  vector<DataSinkPtr> _destinations;
  OperatorPtr _operatorTree;
  bool _compiled = false;

  /**
   * @brief method for serialization, all listed variable below are added to the
   * serialization/deserialization process
   */
  friend class boost::serialization::access;
  template<class Archive>
  void serialize(Archive &ar,
                 const unsigned int version) {
    ar & _description;
    ar & _schema;
    ar & _sources;
    ar & _destinations;
    ar & _operatorTree;
    ar & _compiled;
  }
};

}

#endif //IOTDB_INCLUDE_ACTORS_EXECUTABLETRANSFEROBJECT_HPP_
