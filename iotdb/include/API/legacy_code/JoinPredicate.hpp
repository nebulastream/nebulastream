#ifndef API_JOIN_PREDICATE_H
#define API_JOIN_PREDICATE_H

#include <string>
#include <vector>

#include "Predicate.hpp"
#include "Schema.hpp"
#include <Operators/Operator.hpp>

namespace iotdb {

class JoinPredicate {
  public:
    JoinPredicate(std::string left_side_id, std::string right_side_id, Predicate&& predicate)
        : left_side_id(left_side_id), right_side_id(right_side_id), predicate(predicate), pipeline(0){};

    virtual ~JoinPredicate(){};

    std::string left_side_id;
    std::string right_side_id;

    Predicate& predicate;

    virtual std::string to_string() { return "Join"; };

  protected:
    size_t pipeline;
    virtual std::string generate() = 0;
};

class EquiJoin : public JoinPredicate {
  public:
    EquiJoin(std::string left_side_id, std::string right_side_id)
        : JoinPredicate(left_side_id, right_side_id, Equal(left_side_id, right_side_id))
    {
    }

    std::string to_string() override
    {
        auto string_representation = std::string("Equi-Join[");
        string_representation += left_side_id + std::string(", ");
        string_representation += right_side_id + std::string("]");
        return string_representation;
    };

  protected:
    std::string generate() override;
};
} // namespace iotdb
#endif // API_JOIN_PREDICATE_H
