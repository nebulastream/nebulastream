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
	PrintSink(const Schema& schema);
  ~PrintSink();
  virtual void setup(){};
  virtual void shutdown(){};
  bool writeData(const std::vector<TupleBufferPtr> &input_buffers) override;
  bool writeData(const TupleBufferPtr input_buffer) override;
  const std::string toString() const override;

private:
};


class YSBPrintSink : public PrintSink {
public:
	YSBPrintSink(const Schema& schema);

	~YSBPrintSink();

  bool writeData(const std::vector<TupleBufferPtr> &input_buffers) override;
  bool writeData(const TupleBufferPtr input_buffer) override;
  void setup(){};
  void shutdown(){};
  const std::string toString() const override;

private:
};

} // namespace iotdb



#endif // ZMQSINK_HPP
