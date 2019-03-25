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
    bool writeData(const std::vector<TupleBufferPtr>& input_buffers) override;
    bool writeData(const TupleBufferPtr input_buffer) override;
    bool writeData(const std::vector<TupleBuffer*>& input_buffers) override;
    bool writeData(const TupleBuffer* input_buffer);

    const std::string toString() const override;

  protected:
    size_t printedTuples;
};

class YSBPrintSink : public PrintSink {
  public:
    YSBPrintSink(const Schema& schema);

    ~YSBPrintSink();

    bool writeData(const std::vector<TupleBufferPtr>& input_buffers) override;
    bool writeData(const TupleBufferPtr input_buffer) override;
    bool writeData(const std::vector<TupleBuffer*>& input_buffers) override;
    bool writeData(const TupleBuffer* input_buffer);

    void setup(){};
    void shutdown(){};
    size_t getNumberOfPrintedTuples() { return printedTuples; };

    const std::string toString() const override;

  private:
};

} // namespace iotdb

#endif // ZMQSINK_HPP
