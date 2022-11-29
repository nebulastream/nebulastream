//
// Created by nils on 11/29/22.
//

// Create here a vector of tuples that contains all of the join tuples
std::vector<Nautilus::Record> joinBuffers;
for (auto i = 0UL; i < numberOfTuplesToProduce; ++i) {
    auto recordLeft = Nautilus::Record({{"f1_left", Value<>(i)}, {"f2_left", Value<>(i % 10)}, {"timestamp", Value<>(i)}});
    for (auto j = 0UL; j < numberOfTuplesToProduce; ++j) {
        auto recordRight = Nautilus::Record({{"f1_right", Value<>(i)}, {"f2_right", Value<>(i % 10)}, {"timestamp", Value<>(i)}});
        if (recordLeft.read(joinFieldNameLeft) == recordRight.read(joinFieldNameRight)) {
            auto joinedRecord = Nautilus::Record();
            joinedRecord.write(joinFieldNameLeft, recordLeft.read(joinFieldNameLeft));

            for (auto& field : recordLeft.getAllFields()) {
                joinedRecord.write(field, recordLeft.read(field));
            }
            for (auto& field : recordRight.getAllFields()) {
                joinedRecord.write(field, recordRight.read(field));
            }

            joinBuffers.emplace_back(joinedRecord);
        }
    }
}