#include <Monitoring/MetricValues/MemoryMetrics.hpp>

#include <API/Schema.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Monitoring/Metrics/MetricDefinition.hpp>

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

void serialize(MemoryMetrics metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, MetricDefinition& def,
               const std::string& prefix) {
    if (def.memoryMetrics.find(prefix) != def.memoryMetrics.end()) {
        //element is already in MetricDefinition
        NES_THROW_RUNTIME_ERROR("MemoryMetrics: Error during serialize(..): Metric with " +
            prefix + " is already in MetricDefinition.");
    }
    else {
        def.memoryMetrics.insert(prefix);
        buf.setNumberOfTuples(1);

        auto noFields = schema->getSize();
        schema->copyFields(MemoryMetrics::getSchema(prefix));

        auto layout = createRowLayout(schema);
        layout->getValueField<uint64_t>(0, noFields)->write(buf, metric.TOTAL_RAM);
        layout->getValueField<uint64_t>(0, noFields + 1)->write(buf, metric.TOTAL_SWAP);
        layout->getValueField<uint64_t>(0, noFields + 2)->write(buf, metric.FREE_RAM);
        layout->getValueField<uint64_t>(0, noFields + 3)->write(buf, metric.SHARED_RAM);
        layout->getValueField<uint64_t>(0, noFields + 4)->write(buf, metric.BUFFER_RAM);
        layout->getValueField<uint64_t>(0, noFields + 5)->write(buf, metric.FREE_SWAP);
        layout->getValueField<uint64_t>(0, noFields + 6)->write(buf, metric.TOTAL_HIGH);
        layout->getValueField<uint64_t>(0, noFields + 7)->write(buf, metric.FREE_HIGH);
        layout->getValueField<uint64_t>(0, noFields + 8)->write(buf, metric.PROCS);
        layout->getValueField<uint64_t>(0, noFields + 9)->write(buf, metric.MEM_UNIT);
        layout->getValueField<uint64_t>(0, noFields + 10)->write(buf, metric.LOADS_1MIN);
        layout->getValueField<uint64_t>(0, noFields + 11)->write(buf, metric.LOADS_5MIN);
        layout->getValueField<uint64_t>(0, noFields + 12)->write(buf, metric.LOADS_15MIN);
    }
}

MemoryMetrics MemoryMetrics::fromBuffer(std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    MemoryMetrics output{};
    auto layout = createRowLayout(schema);

    int i = 0;
    for (auto field : schema->fields) {
        if (field->name == prefix + "TOTAL_RAM") {
            break;
        }
        i++;
    }
    if (i <= schema->getSize() && schema->fields[i + 12]->name == prefix + "LOADS_15MIN" && buf.getNumberOfTuples() == 1) {
        output.TOTAL_RAM = layout->getValueField<uint64_t>(0, i)->read(buf);
        output.TOTAL_SWAP = layout->getValueField<uint64_t>(0, i + 1)->read(buf);
        output.FREE_RAM = layout->getValueField<uint64_t>(0, i + 2)->read(buf);
        output.SHARED_RAM = layout->getValueField<uint64_t>(0, i + 3)->read(buf);
        output.BUFFER_RAM = layout->getValueField<uint64_t>(0, i + 4)->read(buf);
        output.FREE_SWAP = layout->getValueField<uint64_t>(0, i + 5)->read(buf);
        output.TOTAL_HIGH = layout->getValueField<uint64_t>(0, i + 6)->read(buf);
        output.FREE_HIGH = layout->getValueField<uint64_t>(0, i + 7)->read(buf);
        output.PROCS = layout->getValueField<uint64_t>(0, i + 8)->read(buf);
        output.MEM_UNIT = layout->getValueField<uint64_t>(0, i + 9)->read(buf);
        output.LOADS_1MIN = layout->getValueField<uint64_t>(0, i + 10)->read(buf);
        output.LOADS_5MIN = layout->getValueField<uint64_t>(0, i + 11)->read(buf);
        output.LOADS_15MIN = layout->getValueField<uint64_t>(0, i + 12)->read(buf);
    } else {
        NES_THROW_RUNTIME_ERROR("MemoryMetrics: Metrics could not be parsed from schema " + schema->toString());
    }
    return output;
}

bool MemoryMetrics::operator==(const MemoryMetrics& rhs) const {
    return TOTAL_RAM == rhs.TOTAL_RAM && TOTAL_SWAP == rhs.TOTAL_SWAP && FREE_RAM == rhs.FREE_RAM
        && SHARED_RAM == rhs.SHARED_RAM && BUFFER_RAM == rhs.BUFFER_RAM && FREE_SWAP == rhs.FREE_SWAP
        && TOTAL_HIGH == rhs.TOTAL_HIGH && FREE_HIGH == rhs.FREE_HIGH && PROCS == rhs.PROCS
        && MEM_UNIT == rhs.MEM_UNIT && LOADS_1MIN == rhs.LOADS_1MIN && LOADS_5MIN == rhs.LOADS_5MIN
        && LOADS_15MIN == rhs.LOADS_15MIN;
}

bool MemoryMetrics::operator!=(const MemoryMetrics& rhs) const {
    return !(rhs == *this);
}

}// namespace NES