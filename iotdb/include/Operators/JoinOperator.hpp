#ifndef OPERATOR_JOIN_OPERATOR_H
#define OPERATOR_JOIN_OPERATOR_H

#include <API/JoinPredicate.hpp>
#include "Operator.hpp"

namespace iotdb {
class JoinOperator : public Operator {
public:

	JoinOperator(JoinPredicate& predicate, Operator* operator_right, Operator* operator_left):
		join_predicate(predicate), operator_right(operator_right), operator_left(operator_left){};

	std::string to_string(){ return join_predicate.to_string(); };

private:
  JoinPredicate &join_predicate;
  Operator *operator_right;
  Operator *operator_left;

};
}
#endif // OPERATOR_JOIN_OPERATOR_H
