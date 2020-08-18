#include <Monitoring/Metrics/Metric.hpp>
#include <Util/Logger.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

void serialize(uint64_t metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->addField(prefix, BasicType::UINT64);
    buf.setNumberOfTuples(1);
    auto layout = createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, metric);
}

void serialize(std::string metric, std::shared_ptr<Schema> schema, TupleBuffer&, const std::string& prefix) {
    NES_THROW_RUNTIME_ERROR("Metric: Serialization for std::string not possible for metric " + metric +
                            "and schema " + prefix + schema->toString());
}

}// namespace NES