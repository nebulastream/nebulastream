#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <Util/Logger.hpp>
#include <API/Schema.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>

namespace NES {

std::shared_ptr<Schema> NES::NetworkValues::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
        ->addField(prefix + "rBytes", BasicType::UINT64)
        ->addField(prefix + "rPackets", BasicType::UINT64)
        ->addField(prefix + "rErrs", BasicType::UINT64)
        ->addField(prefix + "rDrop", BasicType::UINT64)
        ->addField(prefix + "rFifo", BasicType::UINT64)
        ->addField(prefix + "rFrame", BasicType::UINT64)
        ->addField(prefix + "rCompressed", BasicType::UINT64)
        ->addField(prefix + "rMulticast", BasicType::UINT64)
        ->addField(prefix + "tBytes", BasicType::UINT64)
        ->addField(prefix + "tPackets", BasicType::UINT64)
        ->addField(prefix + "tErrs", BasicType::UINT64)
        ->addField(prefix + "tDrop", BasicType::UINT64)
        ->addField(prefix + "tFifo", BasicType::UINT64)
        ->addField(prefix + "tColls", BasicType::UINT64)
        ->addField(prefix + "tCarrier", BasicType::UINT64)
        ->addField(prefix + "tCompressed", BasicType::UINT64);
    return schema;
}

void serialize(NetworkValues metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->copyFields(NetworkValues::getSchema(prefix));

    auto layout = createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, metric.rBytes);
    layout->getValueField<uint64_t>(0, noFields+1)->write(buf, metric.rPackets);
    layout->getValueField<uint64_t>(0, noFields+2)->write(buf, metric.rErrs);
    layout->getValueField<uint64_t>(0, noFields+3)->write(buf, metric.rDrop);
    layout->getValueField<uint64_t>(0, noFields+4)->write(buf, metric.rFifo);
    layout->getValueField<uint64_t>(0, noFields+5)->write(buf, metric.rFrame);
    layout->getValueField<uint64_t>(0, noFields+6)->write(buf, metric.rCompressed);
    layout->getValueField<uint64_t>(0, noFields+7)->write(buf, metric.rMulticast);

    layout->getValueField<uint64_t>(0, noFields+8)->write(buf, metric.tBytes);
    layout->getValueField<uint64_t>(0, noFields+9)->write(buf, metric.tPackets);
    layout->getValueField<uint64_t>(0, noFields+10)->write(buf, metric.tErrs);
    layout->getValueField<uint64_t>(0, noFields+11)->write(buf, metric.tDrop);
    layout->getValueField<uint64_t>(0, noFields+12)->write(buf, metric.tFifo);
    layout->getValueField<uint64_t>(0, noFields+13)->write(buf, metric.tColls);
    layout->getValueField<uint64_t>(0, noFields+14)->write(buf, metric.tCarrier);
    layout->getValueField<uint64_t>(0, noFields+15)->write(buf, metric.tCompressed);
    buf.setNumberOfTuples(1);
}

}// namespace NES
