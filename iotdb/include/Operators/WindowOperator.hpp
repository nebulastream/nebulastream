#ifndef OPERATOR_WINDOW_OPERATOR_H
#define OPERATOR_WINDOW_OPERATOR_H

#include <API/Assigner.hpp>
#include <API/Trigger.hpp>
#include "Operator.hpp"

namespace iotdb {
class WindowOperator : public Operator {
public:
  WindowOperator(Assigner *assigner, Trigger *trigger, Operator *input):
	  assigner(assigner), trigger(trigger), input(input){};
  std::string to_string(){ return "Window " + assigner->to_string() + " " + trigger->to_string(); };

private:
  Assigner *assigner;
  Trigger *trigger;
  Operator *input;
};
}
#endif // OPERATOR_WINDOW_OPERATOR_H
