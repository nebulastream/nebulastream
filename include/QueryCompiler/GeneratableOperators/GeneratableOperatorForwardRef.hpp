#ifndef NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATORFORWARDREF_HPP_
#define NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATORFORWARDREF_HPP_

#include <memory>
namespace NES {

class GeneratableFilterOperator;
typedef std::shared_ptr<GeneratableFilterOperator> GeneratableFilterOperatorPtr;

class GeneratableMapOperator;
typedef std::shared_ptr<GeneratableMapOperator> GeneratableMapOperatorPtr;

class GeneratableMergeOperator;
typedef std::shared_ptr<GeneratableMergeOperator> GeneratableMergeOperatorPtr;

class GeneratableWindowOperator;
typedef std::shared_ptr<GeneratableWindowOperator> GeneratableWindowOperatorPtr;

class GeneratableSinkOperator;
typedef std::shared_ptr<GeneratableSinkOperator> GeneratableSinkOperatorPtr;

class GeneratableSourceOperator;
typedef std::shared_ptr<GeneratableSourceOperator> GeneratableSourceOperatorPtr;

class GeneratableScanOperator;
typedef std::shared_ptr<GeneratableScanOperator> GeneratableScanOperatorPtr;

}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_GENERATABLEOPERATORS_GENERATABLEOPERATORFORWARDREF_HPP_
