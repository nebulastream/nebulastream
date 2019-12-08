#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <API/Config.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>
#include <iostream>
#include <string>
#include <SourceSink/DataSource.hpp>

namespace iotdb {

class Operator;

typedef std::shared_ptr<Operator> OperatorPtr;

class WindowType;
typedef  std::shared_ptr<WindowType> WindowTypePtr;

class WindowAggregation;
typedef  std::shared_ptr<WindowAggregation> WindowAggregationPtr;

/**
 * Interface to create a query.
 */
class InputQuery {
 public:

  InputQuery &operator=(const InputQuery &query);

  InputQuery(const InputQuery &);

  ~InputQuery();

  /**
  * @brief: Creates a query from a particualr stream.
  * @param sourceStream name of the stream to query.
  * @param schema @deprecated will be removed when we have the catalog.
  * @return the input query
  */
  static InputQuery from(Stream &stream);

  /**
   * @brief: Selects a field from the input schema and place it in the output schema.
   * @param field
   * @return query
   */
  InputQuery &select(const Field &field);

  /**
   * @brief: Selects two fields from the input schema and place them in the output schema.
   * @param field
   * @return query
   */
  InputQuery &select(const Field &field1, const Field &field2);

  /**
   * @brief: Filter records according to the predicate.
   * @param predicate
   * @return query
   */
  InputQuery &filter(const UserAPIExpression &predicate);

  /**
   * @brief: Map records to the resultField by the predicate.
   * @param resultField
   * @param predicate
   * @return query
   */
  InputQuery &map(const AttributeField &resultField, const Predicate predicate);

  /**
   * @brief: Unify two queries.
   * All records are contained in the result stream
   * @return query
   */
  InputQuery &combine(const InputQuery &sub_query);

  /**
   * @brief: Joins two streams according to the join predicate.
   * @param sub_query right query.
   * @param joinPred join predicate.
   * @return query.
   */
  InputQuery &join(const InputQuery &sub_query, const JoinPredicatePtr &joinPred);

  /**
   * @brief: Creates a window aggregation.
   * @param windowType Window definition.
   * @param aggregation Window aggregation function.
   * @return query.
   */
  InputQuery &windowByKey(const AttributeFieldPtr onKey, const WindowTypePtr windowType, const WindowAggregationPtr aggregation);


  /**
   * @brief: Creates a window aggregation.
   * @param windowType Window definition.
   * @param aggregation Window aggregation function.
   * @return query.
   */
  InputQuery &window(const WindowTypePtr windowType, const WindowAggregationPtr aggregation);

  /**
  * @brief: Registers the query as a source in the catalog.
  * @param name the name for the result stream.
  */
  InputQuery &to(const std::string &name);

  /**
   * @brief: Adds a file sink, which writes all stream records to the destination file.
   * @param file_name
   */
  InputQuery &writeToFile(const std::string &file_name);

  /**
   * @brief: Adds a zmq sink, which writes all stream records with a schema to a destination zmq.
   * @param schema : schema of the event
   * @param host : host where zmq server is running
   * @param port : port zmq server is listening on
   */
  InputQuery &writeToZmq(const iotdb::Schema &schema, const std::string &host, const uint16_t &port);

  /**
   * Adds a print sink, which prints all query results to the output stream.
   */
  InputQuery &print(std::ostream & = std::cout);

  // helper operators
  OperatorPtr getRoot() const { return root; };

  StreamPtr source_stream;

  int getNextOperatorId() {
    operatorIdCounter++;
    return this->operatorIdCounter;
  }
 private:
  InputQuery(StreamPtr source_stream);
  int operatorIdCounter = 0;
  OperatorPtr root;
};

typedef std::shared_ptr<InputQuery> InputQueryPtr;

/* this function **executes** the code provided by the user and returns an InputQuery Object */
const InputQueryPtr createQueryFromCodeString(const std::string &);

} // namespace iotdb

#endif
