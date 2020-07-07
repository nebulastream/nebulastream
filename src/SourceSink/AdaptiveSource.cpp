#include <SourceSink/AdaptiveSource.hpp>
#include <Util/SensorBus.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

AdaptiveSource::AdaptiveSource(SchemaPtr schema, BufferManagerPtr bufferManager, QueryManagerPtr queryManager, size_t initialFrequency)
    : DataSource(schema, bufferManager, queryManager) {
    NES_DEBUG("AdaptiveSource:" << this << " creating with frequency:" << initialFrequency);
    this->gatheringInterval = initialFrequency;
}

}// namespace NES