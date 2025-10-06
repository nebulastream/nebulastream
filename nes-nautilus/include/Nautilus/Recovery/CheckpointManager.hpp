
#pragma once

#include <cstdint>
#include <cstdio>
#include <future>
#include <string>
#include <vector>
#include <Nautilus/State/Reflection/PipelineState.hpp>
#include <Util/MappedFile.hpp>

namespace NES::Recovery {

class CheckpointManager {
  public:

    /// Serialize arbitrary bytes to a temporary file, then atomically rename.
    static void writeAtomic(const std::string& finalPath, const std::vector<uint8_t>& bytes) {
        const std::string tmpPath = finalPath + ".inprogress";
        /// Write tmp
        FILE* f = std::fopen(tmpPath.c_str(), "wb");
        if (!f) { throw std::runtime_error("Failed to open tmp file: " + tmpPath); }
        if (!bytes.empty()) {
            const size_t n = std::fwrite(bytes.data(), 1, bytes.size(), f);
            if (n != bytes.size()) {
                std::fclose(f);
                throw std::runtime_error("Short write to tmp file: " + tmpPath);
            }
        }
        if (std::fflush(f) != 0) {
            std::fclose(f);
            throw std::runtime_error("Failed to flush tmp file: " + tmpPath);
        }
        if (std::fclose(f) != 0) {
            throw std::runtime_error("Failed to close tmp file: " + tmpPath);
        }
        /// Rename
        if (std::rename(tmpPath.c_str(), finalPath.c_str()) != 0) {
            throw std::runtime_error("Failed to rename tmp file to final: " + finalPath);
        }
    }

    void checkpoint(const State::PipelineState& state, const std::string& path) {
        std::vector<uint8_t> bytes;
        serializePipelineState(state, bytes);
        writeAtomic(path, bytes);
    }

    std::future<void> checkpointAsync(const State::PipelineState& state, const std::string& path) {
        return std::async(std::launch::async, [this, state, path]() {
            this->checkpoint(state, path);
        });
    }

    State::PipelineState recover(const std::string& path) {
        Util::MappedFile mf(path);
        const uint8_t* ptr = mf.data();
        const size_t sz = mf.size();
        if (sz < 16) {
            throw std::runtime_error("Checkpoint file too small or empty: " + path);
        }
        size_t off = 0;
        return deserializePipelineState(ptr, sz, off);
    }

  private:
    /// Little-endian helpers
    static void appendU32(std::vector<uint8_t>& out, uint32_t v) {
        out.push_back(static_cast<uint8_t>(v));
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v >> 16));
        out.push_back(static_cast<uint8_t>(v >> 24));
    }
    static void appendU64(std::vector<uint8_t>& out, uint64_t v) {
        for (int i = 0; i < 8; ++i) out.push_back(static_cast<uint8_t>(v >> (8 * i)));
    }
    static uint32_t readU32(const uint8_t* p, size_t sz, size_t& off) {
        if (off + 4 > sz) throw std::runtime_error("readU32 OOB");
        uint32_t v = p[off] | (p[off + 1] << 8) | (p[off + 2] << 16) | (p[off + 3] << 24);
        off += 4; return v;
    }
    static uint64_t readU64(const uint8_t* p, size_t sz, size_t& off) {
        if (off + 8 > sz) throw std::runtime_error("readU64 OOB");
        uint64_t v = 0; for (int i = 0; i < 8; ++i) v |= (static_cast<uint64_t>(p[off + i]) << (8 * i));
        off += 8; return v;
    }

    /// Simple framing: [magic 4][version u32][queryId u64][pipelineId u64][ts u64]
    /// [opCount u32] { per op: [kind u8][pad3][operatorId u64][stateVersion u32][blobSize u32][blob ...] }
    /// [progressVersion u32][lastWatermark u64][originCount u32] { [originId u64][processed u64][wm u64] }
    static void serializePipelineState(const State::PipelineState& s, std::vector<uint8_t>& out) {
        // Magic
        out.push_back('N'); out.push_back('E'); out.push_back('S'); out.push_back('P');
        appendU32(out, s.version);
        appendU64(out, s.queryId);
        appendU64(out, s.pipelineId);
        appendU64(out, s.createdTimestampNs);
        appendU32(out, static_cast<uint32_t>(s.operators.size()));
        for (const auto& op : s.operators) {
            out.push_back(static_cast<uint8_t>(op.header.kind));
            out.push_back(0); out.push_back(0); out.push_back(0); // padding to 4
            appendU64(out, op.header.operatorId);
            appendU32(out, op.header.version);
            appendU32(out, static_cast<uint32_t>(op.bytes.size()));
            out.insert(out.end(), op.bytes.begin(), op.bytes.end());
        }
        // progress
        appendU32(out, s.progress.version);
        appendU64(out, s.progress.lastWatermark);
        appendU32(out, static_cast<uint32_t>(s.progress.origins.size()));
        for (const auto& o : s.progress.origins) {
            appendU64(out, o.originId);
            appendU64(out, o.processedRecords);
            appendU64(out, o.lastWatermark);
        }
    }

    static State::PipelineState deserializePipelineState(const uint8_t* p, size_t sz, size_t& off) {
        State::PipelineState s;
        // Magic
        if (off + 4 > sz) throw std::runtime_error("Invalid checkpoint header");
        if (!(p[off] == 'N' && p[off + 1] == 'E' && p[off + 2] == 'S' && p[off + 3] == 'P')) {
            throw std::runtime_error("Invalid checkpoint magic");
        }
        off += 4;
        s.version = readU32(p, sz, off);
        s.queryId = readU64(p, sz, off);
        s.pipelineId = readU64(p, sz, off);
        s.createdTimestampNs = readU64(p, sz, off);

        const auto opCount = readU32(p, sz, off);
        s.operators.reserve(opCount);
        for (uint32_t i = 0; i < opCount; ++i) {
            State::OperatorStateBlob blob;
            if (off + 1 > sz) throw std::runtime_error("Corrupt operator header");
            blob.header.kind = static_cast<State::OperatorStateTag::Kind>(p[off]);
            off += 4; // skip kind + padding
            blob.header.operatorId = readU64(p, sz, off);
            blob.header.version = readU32(p, sz, off);
            const auto blobSize = readU32(p, sz, off);
            if (off + blobSize > sz) throw std::runtime_error("Corrupt operator blob size");
            blob.bytes.assign(p + off, p + off + blobSize);
            off += blobSize;
            s.operators.emplace_back(std::move(blob));
        }

        s.progress.version = readU32(p, sz, off);
        s.progress.lastWatermark = readU64(p, sz, off);
        const auto originCount = readU32(p, sz, off);
        s.progress.origins.reserve(originCount);
        for (uint32_t i = 0; i < originCount; ++i) {
            State::ProgressMetadata::OriginProgress o{};
            o.originId = readU64(p, sz, off);
            o.processedRecords = readU64(p, sz, off);
            o.lastWatermark = readU64(p, sz, off);
            s.progress.origins.emplace_back(o);
        }
        // Ensure no trailing bytes (guards corrupted files with appended garbage)
        if (off != sz) {
            throw std::runtime_error("Trailing bytes in checkpoint file");
        }
        return s;
    }
};

}
