//
// Created by pgrulich on 07.05.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_

#include "Nautilus/Interface/DataTypes/Integer/Int.hpp"
#include "Nautilus/Interface/FunctionCall.hpp"
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief A time function, infers the timestamp of an record.
 * For ingestion time, this is determined by the creation ts in the buffer.
 * For event time, this is infered by a field in the record.
 */
class TimeFunction {
  public:
    virtual void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) = 0;
    virtual Nautilus::Value<UInt64> getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record) = 0;
    virtual ~TimeFunction() = default;
};

/**
 * @brief Time function for event time windows
 */
class EventTimeFunction final : public TimeFunction {
  public:
    explicit EventTimeFunction(Expressions::ExpressionPtr timestampExpression);
    void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) override;
    Nautilus::Value<UInt64> getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record) override;

  private:
    Expressions::ExpressionPtr timestampExpression;
};

/**
 * @brief Time function for ingestion time windows
 */
class IngestionTimeFunction final : public TimeFunction {
  public:
    void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) override;
    Nautilus::Value<UInt64> getTs(Execution::ExecutionContext& ctx, Nautilus::Record& record) override;
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_
