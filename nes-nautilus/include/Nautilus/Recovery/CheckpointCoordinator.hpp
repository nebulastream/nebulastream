
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <Nautilus/Recovery/CheckpointManager.hpp>
#include <Nautilus/State/Reflection/PipelineState.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>

#include <Aggregation/SerializableAggregationOperatorHandler.hpp>
#include <Join/HashJoin/SerializableHJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/SerializableNLJOperatorHandler.hpp>

namespace NES::Recovery {

struct OperatorDescriptor {
    uint64_t operatorId{0};
    State::OperatorStateTag::Kind kind{State::OperatorStateTag::Kind::Unknown};
};

class CheckpointCoordinator {
  public:
    static State::PipelineState buildPipelineState(
        uint64_t queryId,
        uint64_t pipelineId,
        uint64_t createdTimestampNs,
        const std::vector<std::shared_ptr<OperatorHandler>>& handlers,
        const std::vector<OperatorDescriptor>& descriptors,
        const State::ProgressMetadata& progress,
        AbstractBufferProvider* bufferProvider) {

        if (handlers.size() != descriptors.size()) {
            throw std::runtime_error("Handlers and descriptors size mismatch");
        }

        State::PipelineState ps{};
        ps.version = 1;
        ps.queryId = queryId;
        ps.pipelineId = pipelineId;
        ps.createdTimestampNs = createdTimestampNs;
        ps.progress = progress;
        ps.operators.reserve(handlers.size());

        for (size_t i = 0; i < handlers.size(); ++i) {
            const auto& h = handlers[i];
            const auto& d = descriptors[i];
            State::OperatorStateBlob blob{};
            blob.header.kind = d.kind;
            blob.header.operatorId = d.operatorId;
            blob.header.version = 1;

            switch (d.kind) {
                case State::OperatorStateTag::Kind::Aggregation: {
                    auto* agg = dynamic_cast<SerializableAggregationOperatorHandler*>(h.get());
                    if (!agg) {
                        throw std::runtime_error("Descriptor kind mismatch: Aggregation expected");
                    }
                    // Ensure we snapshot live slice/window state before serialization
                    agg->captureState();
                    auto tb = agg->serialize(bufferProvider);
                    auto sz = tb.getBufferSize();
                    blob.bytes.resize(sz);
                    if (sz > 0) {
                        std::memcpy(blob.bytes.data(), tb.getBuffer(), sz);
                    }
                    break;
                }
                case State::OperatorStateTag::Kind::HashJoin: {
                    auto* hj = dynamic_cast<SerializableHJOperatorHandler*>(h.get());
                    if (!hj) { blob.bytes.clear(); break; }
                    hj->captureState();
                    auto tb = hj->serialize(bufferProvider);
                    auto sz = tb.getBufferSize();
                    blob.bytes.resize(sz);
                    if (sz > 0) {
                        std::memcpy(blob.bytes.data(), tb.getBuffer(), sz);
                    }
                    break;
                }
                case State::OperatorStateTag::Kind::NestedLoopJoin: {
                    auto* nlj = dynamic_cast<SerializableNLJOperatorHandler*>(h.get());
                    if (!nlj) { blob.bytes.clear(); break; }
                    nlj->captureState();
                    auto tb = nlj->serialize(bufferProvider);
                    auto sz = tb.getBufferSize();
                    blob.bytes.resize(sz);
                    if (sz > 0) {
                        std::memcpy(blob.bytes.data(), tb.getBuffer(), sz);
                    }
                    break;
                }
                // Note: Join kinds handled above when serializable handlers are present
                default:
                    throw std::runtime_error("Unsupported operator kind in CheckpointCoordinator");
            }
            ps.operators.emplace_back(std::move(blob));
        }
        return ps;
    }

    static void writeCheckpoint(const State::PipelineState& ps, const std::string& path) {
        CheckpointManager cm;
        cm.checkpoint(ps, path);
    }
};

}
