#ifndef PRINTSINK_HPP
#define PRINTSINK_HPP

#include <cstdint>
#include <memory>
#include <string>

#include <Runtime/DataSink.hpp>

namespace iotdb {

class PrintSink : public DataSink {

  public:
    PrintSink();
    PrintSink(const Schema& schema);
    ~PrintSink();
    virtual void setup() override {}
    virtual void shutdown() override {}

    bool writeData(const TupleBuffer* input_buffer) override;
    const std::string toString() const override;

  protected:
    friend class boost::serialization::access;

    template <class Archive> void serialize(Archive& ar, const unsigned int version)
    {
        ar& boost::serialization::base_object<DataSink>(*this);
    }
};

} // namespace iotdb

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/export.hpp>
BOOST_CLASS_EXPORT_KEY(iotdb::PrintSink)

#endif // PRINTSINK_HPP
