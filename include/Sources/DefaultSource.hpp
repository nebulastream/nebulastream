#ifndef INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_
#define INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_
#include <Sources/DataSource.hpp>
#include <Sources/GeneratorSource.hpp>

namespace NES {

class DefaultSource : public GeneratorSource {
  public:
    DefaultSource() = default;
    DefaultSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const uint64_t numbersOfBufferToProduce, size_t frequency);

    SourceType getType() const override;

    std::optional<TupleBuffer> receiveData() override;
};

typedef std::shared_ptr<DefaultSource> DefaultSourcePtr;

}// namespace NES
#endif /* INCLUDE_SOURCESINK_DEFAULTSOURCE_HPP_ */
