#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace NES {

class SourceOperator : public Operator {
  public:
    SourceOperator();

    SourceOperator(const DataSourcePtr source);
    SourceOperator(const SourceOperator& other);
    SourceOperator& operator=(const SourceOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    DataSourcePtr getDataSourcePtr();
    ~SourceOperator() override;

  private:
    DataSourcePtr source;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(Operator)
            & BOOST_SERIALIZATION_NVP(source);
    }
};

typedef std::shared_ptr<SourceOperator> SourceOperatorPtr;

}// namespace NES
