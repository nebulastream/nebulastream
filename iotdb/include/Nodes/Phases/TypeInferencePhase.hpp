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

/**
 * @brief The type inference phase receives and query plan and infers all input and output schemata for all operators.
 * If this is not possible it throws an runtime exception.
 */
class TypeInferencePhase {
  public:
    static TypeInferencePhasePtr create();
    QueryPlanPtr transform(QueryPlanPtr queryPlan);
  private:
    SourceDescriptorPtr createSourceDescriptor(std::string streamName);
    TypeInferencePhase();
};
}

#endif //NES_INCLUDE_NODES_PHASES_TYPEINFERENCEPHASE_HPP_
