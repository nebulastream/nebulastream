#include <Monitoring/MetricValues/CpuValues.hpp>

#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

std::shared_ptr<Schema> CpuValues::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
        ->addField(prefix + "USER", BasicType::UINT64)
        ->addField(prefix + "NICE", BasicType::UINT64)
        ->addField(prefix + "SYSTEM", BasicType::UINT64)
        ->addField(prefix + "IDLE", BasicType::UINT64)
        ->addField(prefix + "IOWAIT", BasicType::UINT64)
        ->addField(prefix + "IRQ", BasicType::UINT64)
        ->addField(prefix + "SOFTIRQ", BasicType::UINT64)
        ->addField(prefix + "STEAL", BasicType::UINT64)
        ->addField(prefix + "GUEST", BasicType::UINT64)
        ->addField(prefix + "GUESTNICE", BasicType::UINT64);
    return schema;
}

void serialize(CpuValues metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->copyFields(CpuValues::getSchema(prefix));

    auto layout = createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, metric.USER);
    layout->getValueField<uint64_t>(0, noFields+1)->write(buf, metric.NICE);
    layout->getValueField<uint64_t>(0, noFields+2)->write(buf, metric.SYSTEM);
    layout->getValueField<uint64_t>(0, noFields+3)->write(buf, metric.IDLE);
    layout->getValueField<uint64_t>(0, noFields+4)->write(buf, metric.IOWAIT);
    layout->getValueField<uint64_t>(0, noFields+5)->write(buf, metric.IRQ);
    layout->getValueField<uint64_t>(0, noFields+6)->write(buf, metric.SOFTIRQ);
    layout->getValueField<uint64_t>(0, noFields+7)->write(buf, metric.STEAL);
    layout->getValueField<uint64_t>(0, noFields+8)->write(buf, metric.GUEST);
    layout->getValueField<uint64_t>(0, noFields+9)->write(buf, metric.GUESTNICE);
    buf.setNumberOfTuples(1);
}

}
