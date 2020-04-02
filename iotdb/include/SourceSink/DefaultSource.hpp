#ifndef INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_
#define INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_
#include <SourceSink/DataSource.hpp>
#include <SourceSink/GeneratorSource.hpp>

namespace NES {

class DefaultSource : public GeneratorSource {
  public:
  DefaultSource() = default;
  DefaultSource(SchemaPtr schema, const uint64_t numbersOfBufferToProduce, size_t frequency);

    std::optional<TupleBuffer> receiveData() override;

  private:
    friend class boost::serialization::access;
    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & boost::serialization::base_object<GeneratorSource>(*this);
    }
};

}
#endif /* INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_ */
