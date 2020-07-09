#ifndef NES_INCLUDE_NODES_PHASES_TYPEINFERENCEPHASE_HPP_
#define NES_INCLUDE_NODES_PHASES_TYPEINFERENCEPHASE_HPP_
#include <memory>
namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class Node;
typedef std::shared_ptr<Node> NodePtr;
class TypeInferencePhase;
typedef std::shared_ptr<TypeInferencePhase> TypeInferencePhasePtr;
class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;
class StreamCatalog;
typedef std::shared_ptr<StreamCatalog> StreamCatalogPtr;

/**
 * @brief The type inference phase receives and query plan and infers all input and output schemata for all operators.
 * If this is not possible it throws an runtime exception.
 */
class TypeInferencePhase {
  public:
    /**
     * @brief Factory method to create a type inference phase.
     * @return TypeInferencePhasePtr
     */
    static TypeInferencePhasePtr create(StreamCatalogPtr streamCatalog);

    /**
     * @brief Performs type inference on the given query plan.
     * This involves the following steps.
     * 1. Replacing a logical stream source descriptor with the correct source descriptor form the stream catalog.
     * 2. Propagate the input and output schemas from source operators to the sink operators.
     * 3. If a operator contains expression, we infer the result stamp of this operators.
     * @param QueryPlanPtr the query plan
     * @throws RuntimeException if it was not possible to infer the data types of schemas and expression
     * @return QueryPlanPtr
     */
    QueryPlanPtr execute(QueryPlanPtr queryPlan);

  private:
    /**
     * @brief creates the corresponding source descriptor from a given stream name.
     * @param logicalStreamName
     * @return SourceDescriptorPtr
     */
    SourceDescriptorPtr createSourceDescriptor(std::string streamName);
    explicit TypeInferencePhase(StreamCatalogPtr streamCatalog);
    StreamCatalogPtr streamCatalog;
};
}// namespace NES

#endif//NES_INCLUDE_NODES_PHASES_TYPEINFERENCEPHASE_HPP_
