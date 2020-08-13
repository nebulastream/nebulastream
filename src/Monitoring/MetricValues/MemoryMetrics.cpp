#include <Monitoring/MetricValues/MemoryMetrics.hpp>

#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

std::shared_ptr<Schema> MemoryMetrics::getSchema(const std::string& prefix) {
    auto schema = Schema::create()
        ->addField(prefix + "TOTAL_RAM", BasicType::UINT64)
        ->addField(prefix + "TOTAL_SWAP", BasicType::UINT64)
        ->addField(prefix + "FREE_RAM", BasicType::UINT64)
        ->addField(prefix + "SHARED_RAM", BasicType::UINT64)
        ->addField(prefix + "BUFFER_RAM", BasicType::UINT64)
        ->addField(prefix + "FREE_SWAP", BasicType::UINT64)
        ->addField(prefix + "TOTAL_HIGH", BasicType::UINT64)
        ->addField(prefix + "FREE_HIGH", BasicType::UINT64)
        ->addField(prefix + "PROCS", BasicType::UINT64)
        ->addField(prefix + "MEM_UNIT", BasicType::UINT64)
        ->addField(prefix + "LOADS_1MIN", BasicType::UINT64)
        ->addField(prefix + "LOADS_5MIN", BasicType::UINT64)
        ->addField(prefix + "LOADS_15MIN", BasicType::UINT64);
    return schema;
}

void serialize(MemoryMetrics metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->copyFields(MemoryMetrics::getSchema(prefix));

    auto layout = createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, metric.TOTAL_RAM);
    layout->getValueField<uint64_t>(0, noFields+1)->write(buf, metric.TOTAL_SWAP);
    layout->getValueField<uint64_t>(0, noFields+2)->write(buf, metric.FREE_RAM);
    layout->getValueField<uint64_t>(0, noFields+3)->write(buf, metric.SHARED_RAM);
    layout->getValueField<uint64_t>(0, noFields+4)->write(buf, metric.BUFFER_RAM);
    layout->getValueField<uint64_t>(0, noFields+5)->write(buf, metric.FREE_SWAP);
    layout->getValueField<uint64_t>(0, noFields+6)->write(buf, metric.TOTAL_HIGH);
    layout->getValueField<uint64_t>(0, noFields+7)->write(buf, metric.FREE_HIGH);
    layout->getValueField<uint64_t>(0, noFields+8)->write(buf, metric.PROCS);
    layout->getValueField<uint64_t>(0, noFields+9)->write(buf, metric.MEM_UNIT);
    layout->getValueField<uint64_t>(0, noFields+10)->write(buf, metric.LOADS_1MIN);
    layout->getValueField<uint64_t>(0, noFields+11)->write(buf, metric.LOADS_5MIN);
    layout->getValueField<uint64_t>(0, noFields+12)->write(buf, metric.LOADS_15MIN);
    buf.setNumberOfTuples(1);
}

}