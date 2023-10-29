#include <Util/QuerySignatures/QuerySignatureUtil.hpp>
#include <Util/QuerySignatures/Z3QuerySignatureContext.hpp>

namespace NES::Optimizer {

Z3QuerySignatureContext::Z3QuerySignatureContext(const z3::ContextPtr& context) : context(context) {}

QuerySignaturePtr Z3QuerySignatureContext::createQuerySignatureForOperator(const OperatorNodePtr& operatorNode) const {
    return QuerySignatureUtil::createQuerySignatureForOperator(context, operatorNode);
}

z3::ContextPtr Z3QuerySignatureContext::getContext() const { return context; }

}// namespace NES::Optimizer