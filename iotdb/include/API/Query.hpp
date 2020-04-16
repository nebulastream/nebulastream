#ifndef API_INPUT_QUERY_H
#define API_INPUT_QUERY_H

#include <API/ExpressionAPI.hpp>
#include <API/Config.hpp>
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
     * @brief: Filter records according to the predicate.
     * @param predicate
     * @return query
     */
    Query& filter(const ExpressionNodePtr filterExpression);

    /**
     * @brief: Map records to the resultField by the predicate.
     * @param resultField
     * @param predicate
     * @return query
     *
     * @Caution : The method is not implemented yet.
     */
    Query& map(const ExpressionNodePtr mapExpression);

    /**
     * @brief: Unify two queries.
     * All records are contained in the result stream
     * @return query
     *
     * @Caution : The method is not implemented yet.
     */
    Query& combine(const Query& subQuery);

    /**
     * @brief: Joins two streams according to the join predicate.
     * @param subQuery right query.
     * @param joinPred join predicate.
     * @return query.
     *
     * @Caution : The method is not implemented yet.
     */
    Query& join(const Query& subQuery,
                const JoinPredicatePtr joinPred);

    /**
     * @brief: Creates a window by a key.
     *
     * @Caution : The method is not implemented yet.
     */
    Query& windowByKey();

    /**
     * @brief: Creates a window aggregation.
     *
     * @Caution : The method is not implemented yet.
     */
    Query& window();

    /**
     * @brief: Registers the query as a source in the catalog.
     * @param name the name for the result stream.
     *
     * @Caution : The method is not implemented yet.
     */
    Query& to(const std::string& name);

    /**
     * @brief: Adds a file sink, which writes all stream records to the destination file.
     * @param fileName
     */
    Query& writeToFile(const std::string& fileName);

    /**
     * @brief: Adds a file sink, which writes all stream records to the destination file.
     * @param fileName
     * @param outputMode
     */
    Query& writeToCSVFile(const std::string& fileName, const std::string& outputMode);

    /**
     * @brief: Adds a zmq sink, which writes all stream records with a schema to a destination zmq.
     * @param stream from where to get the schema
     * @param host : host where zmq server is running
     * @param port : port zmq server is listening on
     */
    Query& writeToZmq(const std::string& logicalStreamName, const std::string& host, const uint16_t& port);

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

    /**
     * @brief Get next free operator id
     */
    size_t getNextOperatorId();

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

    /**
     * @brief Returns string representation of the query.
     */
    std::string toString();

  private:

    Query(StreamPtr sourceStreamPtr);
    size_t operatorIdCounter;
    StreamPtr sourceStream;
    OperatorNodePtr rootNode;
    void assignOperatorIdAndSwitchTheRoot(OperatorNodePtr op);
};

typedef std::shared_ptr<Query> QueryPtr;

}  // namespace NES

#endif
