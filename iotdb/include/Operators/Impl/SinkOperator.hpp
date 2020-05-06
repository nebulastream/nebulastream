

#pragma once

#include <memory>
#include <string>

#include <API/ParameterTypes.hpp>
#include <Operators/Operator.hpp>

namespace NES {

class SinkOperator : public Operator {
  public:
    SinkOperator();

    SinkOperator(const DataSinkPtr sink);
    SinkOperator(const SinkOperator& other);
    SinkOperator& operator=(const SinkOperator& other);
    void produce(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    void consume(CodeGeneratorPtr codegen, PipelineContextPtr context, std::ostream& out) override;
    const OperatorPtr copy() const override;
    const std::string toString() const override;
    OperatorType getOperatorType() const override;
    DataSinkPtr getDataSinkPtr();
    ~SinkOperator() override;

  private:
    DataSinkPtr sink_;

    friend class boost::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, unsigned) {
        ar& BOOST_SERIALIZATION_BASE_OBJECT_NVP(Operator)
            & BOOST_SERIALIZATION_NVP(sink_);
    }
};

typedef std::shared_ptr<SinkOperator> SinkOperatorPtr;
}// namespace NES
