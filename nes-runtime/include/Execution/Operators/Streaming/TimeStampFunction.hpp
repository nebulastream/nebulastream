//
// Created by pgrulich on 07.05.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>
namespace NES::Runtime::Execution::Operators {

class TimeStampFunction {
  public:
    virtual void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer) = 0;
    virtual Nautilus::Value<> getTimeStamp(Nautilus::Record& record) = 0;
};

class EventTimeStampFunction : public TimeStampFunction {
  public:
    virtual void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer);
    virtual Nautilus::Value<> getTimeStamp(Nautilus::Record& record);
};

class IngestionTimeStampFunction : public TimeStampFunction {
  public:
    virtual void open(Execution::ExecutionContext& ctx, Execution::RecordBuffer& buffer);
    virtual Nautilus::Value<> getTimetamp(Nautilus::Record& record);
};

}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_TIMEEXTRACTIONFUNCTION_HPP_
