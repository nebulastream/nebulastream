#ifndef NES_INCLUDE_SOURCES_YSBSOURCE_HPP_
#define NES_INCLUDE_SOURCES_YSBSOURCE_HPP_

#include <Sources/DefaultSource.hpp>

namespace NES {

// 78 bytes
static const auto YSB_SCHEMA = Schema::create()
    ->addField("user_id", DataTypeFactory::createFixedChar(16))
    ->addField("page_id", DataTypeFactory::createFixedChar(16))
    ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
    ->addField("ad_type", DataTypeFactory::createFixedChar(9))
    ->addField("event_type", DataTypeFactory::createFixedChar(9))
    ->addField("current_ms", UINT64)
    ->addField("ip", INT32);


class YSBSource : public DefaultSource {
  public:
    explicit YSBSource(BufferManagerPtr bufferManager, QueryManagerPtr queryManager, const uint64_t numbersOfBufferToProduce, size_t frequency, size_t numberOfTuplesPerBuffer);

    SourceType getType() const override;

    std::optional<TupleBuffer> receiveData() override;

    /**
     * @brief override the toString method for the binary source
     * @return returns string describing the binary source
     */
    const std::string toString() const override;

  private:
    size_t numberOfTuplesPerBuffer;
};

typedef std::shared_ptr<YSBSource> YSBSourcePtr;
};// namespace NES

#endif//NES_INCLUDE_SOURCES_YSBSOURCE_HPP_
