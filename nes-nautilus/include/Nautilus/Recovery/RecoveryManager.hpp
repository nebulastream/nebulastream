
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <Nautilus/Recovery/CheckpointManager.hpp>
#include <Nautilus/State/Reflection/PipelineState.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Aggregation/SerializableAggregationOperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Join/HashJoin/SerializableHJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/SerializableNLJOperatorHandler.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/NestedLoopJoin/NLJOperatorHandler.hpp>

namespace NES::Recovery {

struct OperatorConstructionArgs {
    std::vector<OriginId> inputOrigins;
    OriginId outputOriginId{0};
    std::function<std::unique_ptr<WindowSlicesStoreInterface>()> makeSliceStore; // invoked per recover
};

class RecoveryManager {
  public:
    struct RecreateArgs {
        AbstractBufferProvider* bufferProvider{nullptr};
        std::unordered_map<uint64_t, OperatorConstructionArgs> operatorArgsById; // key: operatorId
        // Optional per-operator factory to construct handlers (preferred for joins)
        std::unordered_map<uint64_t, std::function<std::shared_ptr<OperatorHandler>(const State::OperatorStateBlob& blob)>> operatorFactoryById;
    };

    struct RecoveredPipeline {
        std::vector<std::shared_ptr<OperatorHandler>> handlers;
        State::PipelineState pipelineState;
    };

    // Load checkpoint and rebuild operator handlers using provided construction args
    static RecoveredPipeline recoverPipeline(const std::string& checkpointPath, const RecreateArgs& args) {
        CheckpointManager cm;
        auto ps = cm.recover(checkpointPath);
        std::vector<std::shared_ptr<OperatorHandler>> handlers;
        handlers.reserve(ps.operators.size());

        for (const auto& blob : ps.operators) {
            auto it = args.operatorArgsById.find(blob.header.operatorId);
            if (it == args.operatorArgsById.end()) {
                throw std::runtime_error("Missing OperatorConstructionArgs for operatorId=" + std::to_string(blob.header.operatorId));
            }
            const auto& carg = it->second;

            // Create TupleBuffer from blob bytes
            if (!args.bufferProvider) {
                throw std::runtime_error("RecoveryManager requires a valid AbstractBufferProvider");
            }
            auto bufOpt = args.bufferProvider->getUnpooledBuffer(blob.bytes.size());
            if (!bufOpt) {
                throw std::runtime_error("Failed to allocate TupleBuffer for operator state");
            }
            auto tb = bufOpt.value();
            if (!blob.bytes.empty()) {
                std::memcpy(tb.getBuffer(), blob.bytes.data(), blob.bytes.size());
            }

            // Prefer user-provided factory for reconstruction
            if (auto fit = args.operatorFactoryById.find(blob.header.operatorId); fit != args.operatorFactoryById.end()) {
                handlers.emplace_back(fit->second(blob));
                continue;
            }

            // Built-in fallback support for Aggregation
            if (blob.header.kind == State::OperatorStateTag::Kind::Aggregation) {
                auto handler = SerializableAggregationOperatorHandler::deserialize(
                    tb, carg.inputOrigins, carg.outputOriginId, carg.makeSliceStore());
                // Initialize watermark baselines from serialized state and pipeline progress
                if (handler) {
                    // Seed per-origin baselines if present
                    if (!ps.progress.origins.empty()) {
                        for (const auto& op : ps.progress.origins) {
                            handler->seedBuildWatermarkForOrigin(OriginId(op.originId), Timestamp(op.lastWatermark));
                        }
                        // Probe baseline uses overall progress watermark (min will be applied internally)
                        handler->setProbeWatermarkBaseline(Timestamp(ps.progress.lastWatermark));
                    } else {
                        uint64_t wm = ps.progress.lastWatermark;
                        if (wm == 0) {
                            const auto& st = handler->getState();
                            wm = st.metadata.lastWatermark;
                        }
                        Timestamp baseline(wm);
                        handler->setBuildWatermarkBaseline(baseline);
                        handler->setProbeWatermarkBaseline(baseline);
                    }
                }
                handlers.emplace_back(std::move(handler));
            } else if (blob.header.kind == State::OperatorStateTag::Kind::HashJoin) {
                if (blob.bytes.empty()) {
                    // No state; use factory if provided (handled above) or create default HJ handler without state support
                    auto handler = std::make_shared<HJOperatorHandler>(carg.inputOrigins, carg.outputOriginId, carg.makeSliceStore());
                    handlers.emplace_back(std::move(handler));
                } else {
                    auto handler = SerializableHJOperatorHandler::deserialize(
                        tb, carg.inputOrigins, carg.outputOriginId, carg.makeSliceStore());
                    // Seed watermarks similar to aggregation
                    if (handler) {
                        if (!ps.progress.origins.empty()) {
                            for (const auto& op : ps.progress.origins) {
                                handler->seedBuildWatermarkForOrigin(OriginId(op.originId), Timestamp(op.lastWatermark));
                            }
                            handler->setProbeWatermarkBaseline(Timestamp(ps.progress.lastWatermark));
                        } else {
                            uint64_t wm = ps.progress.lastWatermark;
                            Timestamp baseline(wm);
                            handler->setBuildWatermarkBaseline(baseline);
                            handler->setProbeWatermarkBaseline(baseline);
                        }
                        handler->rehydrateFromState();
                    }
                    handlers.emplace_back(std::move(handler));
                }
            } else if (blob.header.kind == State::OperatorStateTag::Kind::NestedLoopJoin) {
                if (blob.bytes.empty()) {
                    auto handler = std::make_shared<NLJOperatorHandler>(carg.inputOrigins, carg.outputOriginId, carg.makeSliceStore());
                    handlers.emplace_back(std::move(handler));
                } else {
                    auto handler = SerializableNLJOperatorHandler::deserialize(
                        tb, carg.inputOrigins, carg.outputOriginId, carg.makeSliceStore());
                    if (handler) {
                        if (!ps.progress.origins.empty()) {
                            for (const auto& op : ps.progress.origins) {
                                handler->seedBuildWatermarkForOrigin(OriginId(op.originId), Timestamp(op.lastWatermark));
                            }
                            handler->setProbeWatermarkBaseline(Timestamp(ps.progress.lastWatermark));
                        } else {
                            uint64_t wm = ps.progress.lastWatermark;
                            Timestamp baseline(wm);
                            handler->setBuildWatermarkBaseline(baseline);
                            handler->setProbeWatermarkBaseline(baseline);
                        }
                        handler->rehydrateFromState();
                    }
                    handlers.emplace_back(std::move(handler));
                }
            } else {
                throw std::runtime_error("Unsupported operator kind in RecoveryManager and no factory provided");
            }
        }

        return {std::move(handlers), std::move(ps)};
    }
};

}
