

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace iotdb {

class FilterOperator : public Operator {
  public:
        FilterOperator() = default;

    FilterOperator(const PredicatePtr& predicate);
    FilterOperator(const FilterOperator& other);
    FilterOperator& operator=(const FilterOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    ~FilterOperator() override;

  private:
    PredicatePtr predicate_;

        friend class boost::serialization::access;

        template<class Archive>
        void serialize(Archive &ar, unsigned) {
            ar & BOOST_SERIALIZATION_BASE_OBJECT_NVP(Operator)
            & BOOST_SERIALIZATION_NVP(predicate_);
        }
};

} // namespace iotdb
