#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

std::shared_ptr<Schema> DiskMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
        ->addField(prefix + "F_BSIZE", BasicType::UINT64)
        ->addField(prefix + "F_FRSIZE", BasicType::UINT64)
        ->addField(prefix + "F_BLOCKS", BasicType::UINT64)
        ->addField(prefix + "F_BFREE", BasicType::UINT64)
        ->addField(prefix + "F_BAVAIL", BasicType::UINT64);
    return schema;
}

void serialize(DiskMetrics metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->copyFields(DiskMetrics::getSchema(prefix));

    auto layout = createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, metric.fBsize);
    layout->getValueField<uint64_t>(0, noFields+1)->write(buf, metric.fFrsize);
    layout->getValueField<uint64_t>(0, noFields+2)->write(buf, metric.fBlocks);
    layout->getValueField<uint64_t>(0, noFields+3)->write(buf, metric.fBfree);
    layout->getValueField<uint64_t>(0, noFields+4)->write(buf, metric.fBavail);
    buf.setNumberOfTuples(1);
}


}