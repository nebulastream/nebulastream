#include <Monitoring/MetricValues/CpuValues.hpp>

#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

std::pair<std::shared_ptr<Schema>, TupleBuffer> CpuValues::serialize(std::shared_ptr<BufferManager> bm,
                                                                     const std::string& prefix) const {
    auto schema = Schema::create();
    TupleBuffer buf = bm->getBufferBlocking();
    serialize(schema, buf, prefix);
    return std::make_pair(schema, buf);
}

void CpuValues::serialize(std::shared_ptr<Schema> schema, TupleBuffer buf, const std::string& prefix) const {
    auto noFields = schema->getSize();
    schema->copyFields(getSchema(prefix));

    auto layout = createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, USER);
    layout->getValueField<uint64_t>(0, noFields+1)->write(buf, NICE);
    layout->getValueField<uint64_t>(0, noFields+2)->write(buf, SYSTEM);
    layout->getValueField<uint64_t>(0, noFields+3)->write(buf, IDLE);
    layout->getValueField<uint64_t>(0, noFields+4)->write(buf, IOWAIT);
    layout->getValueField<uint64_t>(0, noFields+5)->write(buf, IRQ);
    layout->getValueField<uint64_t>(0, noFields+6)->write(buf, SOFTIRQ);
    layout->getValueField<uint64_t>(0, noFields+7)->write(buf, STEAL);
    layout->getValueField<uint64_t>(0, noFields+8)->write(buf, GUEST);
    layout->getValueField<uint64_t>(0, noFields+9)->write(buf, GUESTNICE);
    buf.setNumberOfTuples(1);
}

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

}
