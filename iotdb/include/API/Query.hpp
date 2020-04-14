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

class Operator;

typedef std::shared_ptr<Operator> OperatorPtr;

class WindowType;
typedef std::shared_ptr<WindowType> WindowTypePtr;

class WindowAggregation;
typedef std::shared_ptr<WindowAggregation> WindowAggregationPtr;

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
    Query& filter(const UserAPIExpression& predicate);

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
     * @brief: Creates a window aggregation.
     * @param windowType Window definition.
     * @param aggregation Window aggregation function.
     * @return query.
     */
    Query& windowByKey(const AttributeFieldPtr onKey,
                       const WindowTypePtr windowType,
                       const WindowAggregationPtr aggregation);

    /**
     * @brief: Creates a window aggregation.
     * @param windowType Window definition.
     * @param aggregation Window aggregation function.
     * @return query.
     */
    Query& window(const WindowTypePtr windowType,
                  const WindowAggregationPtr aggregation);

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

    /**
     * @brief: provide a sample operator for sense
     * @param udfs to setup sense
     */
    Query& sample(const std::string& udfs);

    int getNextOperatorId() {
        operatorIdCounter++;
        return this->operatorIdCounter;
    }

    const StreamPtr getSourceStream() const;
    void setSourceStream(const StreamPtr sourceStream);

    // Copy constructors
    Query(const Query&);

    Query& operator=(const Query& query);

  private:
    Query(StreamPtr source_stream);
    int operatorIdCounter = 0;
    StreamPtr sourceStream;

};

typedef std::shared_ptr<Query> QueryPtr;

}  // namespace NES

#endif
