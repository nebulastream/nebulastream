#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <API/Config.hpp>
#include <API/ParameterTypes.hpp>
#include <API/Schema.hpp>
#include <API/Stream.hpp>

#include <string>
#include <SourceSink/DataSource.hpp>
#include <cppkafka/configuration.h>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class ExpressionNode;
typedef std::shared_ptr<ExpressionNode> ExpressionNodePtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

/**
 * Interface to create new query.
 */
class Query {
  public:

    ~Query() = default;

    /**
     * @brief: Creates a query from a particualr stream.
     * @param sourceStream name of the stream to query.
     * @param schema @deprecated will be removed when we have the catalog.
     * @return the input query
     */
    static Query from(Stream& stream);

    /**
     * @brief: Selects a field from the input schema and place it in the output schema.
     * @param field
     * @return query
     */
    Query& select(const Field& field);

    /**
     * @brief: Selects two fields from the input schema and place them in the output schema.
     * @param field
     * @return query
     */
    Query& select(const Field& field1, const Field& field2);

    /**
     * @brief: Filter records according to the predicate.
     * @param predicate
     * @return query
     */
    Query& filter(const ExpressionNodePtr expressionNodePtr);

    /**
     * @brief: Map records to the resultField by the predicate.
     * @param resultField
     * @param predicate
     * @return query
     */
    Query& map(const AttributeField& resultField, const Predicate predicate);

    /**
     * @brief: Unify two queries.
     * All records are contained in the result stream
     * @return query
     */
    Query& combine(const Query& sub_query);

    /**
     * @brief: Joins two streams according to the join predicate.
     * @param sub_query right query.
     * @param joinPred join predicate.
     * @return query.
     */
    Query& join(const Query& sub_query,
                const JoinPredicatePtr joinPred);

    /**
     * @brief: Creates a window by a key.
     */
    Query& windowByKey();

    /**
     * @brief: Creates a window aggregation.
     */
    Query& window();

    /**
     * @brief: Registers the query as a source in the catalog.
     * @param name the name for the result stream.
     */
    Query& to(const std::string& name);

    /**
     * @brief: Adds a file sink, which writes all stream records to the destination file.
     * @param file_name
     */
    Query& writeToFile(const std::string& file_name);

    /**
     * @brief: Adds a file sink, which writes all stream records to the destination file.
     * @param file_name
     */
    Query& writeToCSVFile(const std::string& file_name);

    /**
     * @brief: Adds a zmq sink, which writes all stream records with a schema to a destination zmq.
     * @param stream from where to get the schema
     * @param host : host where zmq server is running
     * @param port : port zmq server is listening on
     */
    Query& writeToZmq(const std::string& logicalStreamName,
                      const std::string& host, const uint16_t& port);

    /**
     * @brief write to a kafka sink
     * @param brokers: broker list
     * @param topic: kafka topic to write to
     * @param kafkaProducerTimeout: kafka producer timeout
     * @return this object
     */
    Query& writeToKafka(const std::string& brokers, const std::string& topic,
                        const size_t kafkaProducerTimeout);

    /**
     * @brief write to a kafka sink
     * @param topic: kafka topic to write to
     * @param config: kafka producer configuration
     * @return this object
     */
    Query& writeToKafka(const std::string& topic,
                        const cppkafka::Configuration& config);

    /**
     * Adds a print sink, which prints all query results to the output stream.
     */
    Query& print(std::ostream& = std::cout);

    int getNextOperatorId() {
        operatorIdCounter++;
        return this->operatorIdCounter;
    }

    /**
     * @brief Get all source operators
     * @return vector of logical source operators
     */
    std::vector<SourceLogicalOperatorNodePtr> getSourceOperators();

    /**
     * @brief Get all sink operators
     * @return vector of logical sink operators
     */
    std::vector<SinkLogicalOperatorNodePtr> getSinkOperators();

    /**
     * @brief get source stream
     * @return pointer to source stream
     */
    const StreamPtr getSourceStream() const;

    // Copy constructors
    Query(const Query&);

    Query& operator=(const Query& query);

  private:

    Query(StreamPtr source_stream);
    int operatorIdCounter = 0;
    StreamPtr sourceStream;
    OperatorNodePtr root;
};

typedef std::shared_ptr<Query> QueryPtr;

}  // namespace NES

#endif
