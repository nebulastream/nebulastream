#ifndef NES_QUERYPLANSERIALIZATIONUTIL_HPP
#define NES_QUERYPLANSERIALIZATIONUTIL_HPP

#include <memory>

namespace NES {

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class SerializableQueryPlan;

class QueryPlanSerializationUtil {
  public:

    /**
     * @brief Serializes a Query Plan and all its root operators to a SerializableQueryPlan object.
     * @param queryPlan: The query plan
     * @return the pointer to serialized SerializableQueryPlan
     */
    static SerializableQueryPlan* serializeQueryPlan(QueryPlanPtr queryPlan);

    /**
     * @brief De-serializes the SerializableQueryPlan and all its root operators back to a QueryPlanPtr
     * @param serializedQueryPlan the serialized query plan.
     * @return the pointer to the deserialized query plan
     */
    static QueryPlanPtr deserializeQueryPlan(SerializableQueryPlan* serializedQueryPlan);
};
}// namespace NES
#endif//NES_QUERYPLANSERIALIZATIONUTIL_HPP
