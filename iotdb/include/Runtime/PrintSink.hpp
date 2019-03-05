#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Runtime/DataSink.hpp>

namespace iotdb {

class PrintSink : public DataSink {

public:
	PrintSink(const Schema &schema);
  ~PrintSink();

  bool writeData(const std::vector<TupleBuffer *> &input_buffers) override;
  const std::string toString() const override;

private:
};
} // namespace iotdb

#endif // ZMQSINK_HPP
