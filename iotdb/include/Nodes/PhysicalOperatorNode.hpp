#ifndef PHYSICAL_OPERATOR_NODE_HPP
#define PHYSICAL_OPERATOR_NODE_HPP

namespace NES {

class PhysicalOperatorNode : public BaseOperatorNode {
public:
    void produce();
    void consume();
private:

};
}
#endif  // PHYSICAL_OPERATOR_NODE_HPP
