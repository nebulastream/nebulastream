//
//  online.cpp
//  Compress
//
//  Created by DB on 3/31/20.
//  Copyright © 2020 D Blalock. All rights reserved.
//

#include "Runtime/MemoryLayout/Compression/Sprintz/online.hpp"
#include "Runtime/MemoryLayout/Compression/Sprintz/format.h"

// #include "array_utils.hpp"
#include "Runtime/MemoryLayout/Compression/Sprintz/debug_utils.hpp"

// =================================================== dynamic predictor choice

template<int LossFunc, int BlockSz=8>
loss_t _compute_loss_u16(const uint16_t* buff) {
    typedef uint16_t int_t; // typedef so easy to adapt code for other types
    static_assert(LossFunc == Losses::MaxAbs || LossFunc == Losses::SumLogAbs,
                  "Invalid loss function!");
    static_assert(BlockSz >= 1, "Blocks must contain at least 1 element!");

    // uint8_t sign_bit_pos = 8 * sizeof(int_t) - 1;

    if (LossFunc == Losses::MaxAbs) {
        // int_t val = *buff;
        // val ^= val >> sign_bit_pos;  // flip bits if negative
        // loss_t loss = ZIGZAG_ENCODE_SCALAR(*buff);
        loss_t loss = *buff;
        for (int i = 1; i < BlockSz; i++) {
            loss = MAX(loss, buff[i]);
        }
        return loss;
    } else if (LossFunc == Losses::SumLogAbs) {
        loss_t loss = 0;
        for (int i = 0; i < BlockSz; i++) {
            uint8_t nleading_zeros = __builtin_clz(buff[i]);
            uint8_t logabs = sizeof(int_t) * 8 - nleading_zeros;
            loss += logabs;
        }
        return loss;
    }
    return -1;
}

template<int LossFunc=Losses::SumLogAbs, int BlockSz=8>
len_t dynamic_delta_zigzag_encode_u16(
    const uint16_t* data_in, int16_t* data_out, uint8_t* choices_out,
    len_t length)
{
    typedef int16_t int_t;
    typedef uint16_t uint_t;

    if (length <= 0) { return length; }
    // always copy the first value
    *data_out = *data_in;
    if (length == 1) { return length; }

    // auto orig_data_out = data_out;

    // ar::print(data_in, "input")

    length -= 1;  // effective length is 1 less, since 1st elem already copied
    len_t nblocks = length / BlockSz;
    len_t full_blocks_len = nblocks * BlockSz;
    len_t tail_len = length - full_blocks_len;

    // allocate buffers for codecs we consider
    uint_t tmp0[BlockSz];
    uint_t tmp1[BlockSz];

    // create encoders
    PredictiveCoder<DeltaPredictor_u16> enc0;
    // PredictiveCoder<DeltaPredictor_u16> enc1;
    PredictiveCoder<DoubleDeltaPredictor_u16> enc1;
    enc0.init(data_in[0]);
    enc1.init(data_in[0]);

    // zero out choices buffer; it's a bitfield so length/8 bytes, rounded up
    auto nheader_bytes = (length + 7) / 8;
    for (uint i = 0; i < nheader_bytes; i++) {
        choices_out[i] = 0;
    }
    if (nheader_bytes % 2) { // ensure deterministic last byte if odd nbytes
        choices_out[nheader_bytes] = 0;
    }

    const uint_t* in_ptr = data_in + 1;
    int_t* out_ptr = data_out + 1;
    for (len_t b = 0; b < nblocks; b++) {
        for (int bb = 0; bb < BlockSz; bb++) {
            uint_t val = *in_ptr++;
            tmp0[bb] = zigzag_encode_16b(enc0.encode_next(val));
            tmp1[bb] = zigzag_encode_16b(enc1.encode_next(val));
            // if (val == 1) {
            //     printf("bb = %d, val = %d\n", bb, val);
            // }
            // if (bb == 7) { printf("enc next prediction: %d\n", enc0._predictor.predict()); }
            // auto err = enc0.encode_next(val);
            // printf("bb = %d, val = %d, err = %d, zz(err) = %d\n", bb, val, err, zigzag_encode_16b(err));
            // tmp0[bb] = zigzag_encode_16b(err);
            // TODO uncomment above
            // tmp0[bb] = enc0.encode_next(*in_ptr);
            // auto val1 = enc1.encode_next(*in_ptr);
            // tmp1[bb] = ZIGZAG_ENCODE_SCALAR(val1);
            // printf("elem, zigzagged elem = %d, %d\n", val1, ZIGZAG_ENCODE_SCALAR(val1));
        }
        // ar::print(&tmp0[0], BlockSz, "tmp0");
        // ar::print(&tmp1[0], BlockSz, "tmp1");
        loss_t loss0 = _compute_loss_u16<LossFunc, BlockSz>(tmp0);
        loss_t loss1 = _compute_loss_u16<LossFunc, BlockSz>(tmp1);

        uint_t* copy_from;
        uint8_t choice;
        if (loss0 <= loss1) {
        // if (true) { // TODO rm  // works
        // if (false) { // TODO rm
            copy_from = tmp0;
            choice = 0;
        } else {
            copy_from = tmp1;
            choice = 1;
        }
        // ar::print(&tmp0[0], BlockSz, "tmp0");
        // ar::print(&tmp1[0], BlockSz, "tmp1");
        // printf("enc choosing codec %d\n", choice);
        // write out prediction errs and choice of predictor
        for (int bb = 0; bb < BlockSz; bb++) {
            // printf("enc writing val: %d (%d)\n", copy_from[bb], ZIGZAG_DECODE_SCALAR(copy_from[bb]));
            // printf("tmp0[bb], tmp1[bb] = %d, %d\n", tmp0[bb], tmp1[bb]);
            *out_ptr++ = copy_from[bb];
        }
        len_t choices_byte_offset = b / 8;
        uint8_t choices_bit_offset = (uint8_t)(b % 8);
        choices_out[choices_byte_offset] |= choice << choices_bit_offset;
    }
    // just delta code the tail
    // printf("enc enc0 next prediction: %d\n", enc0._predictor.predict());
    // printf("enc *in_ptr, *(in_ptr+1): %d, %d\n", *in_ptr, *(in_ptr + 1));
    for (len_t i = 0; i < tail_len; i++) {
        // auto val = enc0.encode_next(*in_ptr++);
        // printf("enc writing tail val: %d\n", val);
        // *out_ptr++ = val;
        *out_ptr++ = enc0.encode_next(*in_ptr++);
    }

    // auto ret_nbytes = ((uint8_t*)data_out) - ((uint8_t*)orig_data_out);
    // auto ret = (len_t)(ret_nbytes + 1) / 2;
    // if (ret_nbytes % 2) {
    //     // if didn't consume an even number of u16s, ensure that the trailing
    //     // byte is deterministic (probably unnecessary by why take chances)
    //     ((uint8_t*)orig_data_out)[ret_nbytes + 1] = 0;
    // }

    // ar::print(data_out, length + 1, "compressed\t\t\t");
    // ar::print(ar::map(zigzag_decode_16b, data_out, length+1).get(), length+1, "compressed nozigzag\t");

    return length + 1; // original length
    // return ret;
}

len_t dynamic_delta_zigzag_decode_u16(
    const int16_t* data_in, len_t length, uint16_t* data_out,
    const uint8_t* choices_in)
{
    typedef uint16_t uint_t;
    typedef int16_t int_t;
    static const int BlockSz = 8; // TODO allow other block sizes?
    static_assert(BlockSz >= 3,
        "Predictor jump() can read past beginning with small blocks");

    if (length <= 0) { return length; }
    // always copy the first value
    *data_out = *data_in;
    if (length == 1) { return length; }

    length -= 1;  // effective length is 1 less, since 1st elem already copied
    len_t nblocks = length / BlockSz;
    len_t full_blocks_len = nblocks * BlockSz;
    len_t tail_len = length - full_blocks_len;

    // create encoders
    PredictiveCoder<DeltaPredictor_u16> enc0;
    PredictiveCoder<DoubleDeltaPredictor_u16> enc1;
    enc0.init(data_in[0]);
    enc1.init(data_in[0]);

    const int_t* in_ptr = data_in + 1;
    uint_t* out_ptr = data_out + 1;
    for (len_t b = 0; b < nblocks; b++) {
        // read the next predictor choice
        len_t choices_byte_offset = b / 8;
        uint8_t choices_bit_offset = (uint8_t)(b % 8);
        uint8_t choice = (
            choices_in[choices_byte_offset] >> choices_bit_offset) & 0x01;
        // printf("dec choosing codec %d\n", choice);
        // use chosen codec to decode this block
        switch(choice) {
            case (0): // delta code
                // printf("dec using delta codec\n");
                for (int bb = 0; bb < BlockSz; bb++) {
                    int_t val = zigzag_decode_16b(*in_ptr++);


                    // SELF: issue when switching coders is that predictive
                    // coder we don't use never sees the right vals; it instead
                    // sees the vals that it would have reconstructed given
                    // the errs; two solutions:
                    //  1) add train() to the predictive coder to let us pass
                    //  in the correct values
                    //  2) add jump() method to let us skip feeding in all the
                    //  values
                    //
                    //  -might be nice to have both; moving avg can't jump
                    // but training on everything unnecessarily is a waste



                    // printf("dec bb = %d, val = %d\n", bb, val);
                    // if (bb == 7) { printf("dec next prediction: %d\n", enc0._predictor.predict()); }
                    *out_ptr++ = enc0.decode_next(val);
                    // enc1.decode_next(val);  // TODO allow skip to block end
                }
                enc1.jump(out_ptr[-1], out_ptr[-2], out_ptr[-3]);
                break;
            case (1): // double delta code
                // printf("dec using double delta codec\n");
                for (int bb = 0; bb < BlockSz; bb++) {
                    int_t val = zigzag_decode_16b(*in_ptr++);
                    // enc0.decode_next(val);
                    *out_ptr++ = enc1.decode_next(val);
                }
                enc0.jump(out_ptr[-1], out_ptr[-2], out_ptr[-3]);
                break;
            default:
                printf("ERROR: got codec choice %d!\n", choice);
                break; // can't happen
        }
    }
    // printf("dec enc0 next prediction: %d\n", enc0._predictor.predict());
    // printf("dec *in_ptr, *(in_ptr+1): %d, %d\n", *in_ptr, *(in_ptr + 1));
    // just delta code the tail
    for (len_t i = 0; i < tail_len; i++) {
        // auto val = enc0.decode_next(*in_ptr++);
        // printf("dec writing tail val: %d\n", val);
        // *out_ptr++ = val;
        *out_ptr++ = enc0.decode_next(*in_ptr++);
    }
    return length + 1; // original length
}

len_t dynamic_delta_choices_size_bytes(len_t length, int blocksz) {
    len_t nblocks = (length + blocksz - 1) / blocksz;
    // return (nblocks + 7) / 8; // numbytes
    auto nbits = nblocks * 1; // one bit per block
    return (nbits + 7) / 8; // numbytes
}

len_t dynamic_delta_zigzag_encode_u16(
    const uint16_t* data_in, len_t length, int16_t* data_out,
    uint8_t* choices_out, int loss)
{
    switch(loss) {
        case (Losses::MaxAbs):
            return dynamic_delta_zigzag_encode_u16<Losses::MaxAbs>(
                data_in, data_out, choices_out, length);
        case (Losses::SumLogAbs):
            return dynamic_delta_zigzag_encode_u16<Losses::SumLogAbs>(
                data_in, data_out, choices_out, length);
        default: return -1;
    }
}

len_t _dynamic_delta_pack_u16(
    const uint16_t* data_in, size_t size, int16_t* data_out, int loss)
{
    len_t length = (len_t)size; // avoid implicit conversion warnings
    // int loss = Losses::SumLogAbs;
    // int loss = Losses::MaxAbs;
    uint16_t offset = write_metadata_simple1d(data_out, length);
    data_out += offset;
    uint8_t* choices_out = (uint8_t*)(data_out + length);
    len_t choices_size = (dynamic_delta_choices_size_bytes(length) + 1) / 2;
    return offset + dynamic_delta_zigzag_encode_u16(
        data_in, length, data_out, choices_out, loss) + choices_size;
}
len_t dynamic_delta_pack_u16(
    const uint16_t* data_in, size_t length, int16_t* data_out)
{
    return _dynamic_delta_pack_u16(
        data_in, length, data_out, Losses::SumLogAbs);
}
len_t dynamic_delta_pack_u16_altloss(
    const uint16_t* data_in, size_t length, int16_t* data_out)
{
    return _dynamic_delta_pack_u16(
        data_in, length, data_out, Losses::MaxAbs);
}

len_t dynamic_delta_unpack_u16(
    const int16_t* data_in, uint16_t* data_out)
{
    len_t length;
    uint16_t offset = read_metadata_simple1d(data_in, &length);
    data_in += offset;
    const uint8_t* choices_in = (const uint8_t*)(data_in + length);
    return dynamic_delta_zigzag_decode_u16(
        data_in, length, data_out, choices_in);
}

// =================================================== just zigzag

len_t zigzag_encode_u16(
    const uint16_t* data_in, len_t length, int16_t* data_out)
{
    for (len_t i = 0; i < length; i++) {
        // *data_out++ = zigzag_encode_16b(*data_in++);
        data_out[i] = zigzag_encode_16b(data_in[i]);
        // *data_out++ = zigzag_decode_16b(*data_in++);
    }
    return length;
}

len_t zigzag_decode_u16(
    const int16_t* data_in, len_t length, uint16_t* data_out)
{
    for (len_t i = 0; i < length; i++) {
        // *data_out++ = zigzag_decode_16b(*data_in++);
        data_out[i] = zigzag_decode_16b(data_in[i]);
    }
    return length;
}

len_t zigzag_pack_u16(
    const uint16_t* data_in, size_t size, int16_t* data_out)
{
    len_t length = (len_t)size; // avoid implicit conversion warnings
    uint16_t offset = write_metadata_simple1d(data_out, length);
    data_out += offset;
    return offset + zigzag_encode_u16(data_in, length, data_out);
}
len_t zigzag_unpack_u16(
    const int16_t* data_in, uint16_t* data_out)
{
    len_t length;
    uint16_t offset = read_metadata_simple1d(data_in, &length);
    data_in += offset;
    return zigzag_decode_u16(data_in, length, data_out);
}


// =================================================== just sprintz bitpack

len_t sprintzpack_headers_size_bytes_u16(len_t length, int blocksz) {
    len_t nblocks = (length + blocksz - 1) / blocksz;
    auto nbits = nblocks * 4;
    return (nbits + 7) / 8; // numbytes
}

// template<int BlockSz=8>
// template<int BlockSz=8, bool ZigZag=false>
template<bool ZigZag=true, int BlockSz=8>
len_t _sprintzpack_encode_u16(
    const uint16_t* data_in, len_t length, int16_t* data_out,
    uint8_t* headers_out)
{
    typedef int16_t int_t;
    typedef uint16_t uint_t;
    static constexpr int nbits_sz_bits = 4;
    // assert(BlockSz % 4 == 0); // our impl writes to uint64s to do output
    assert(BlockSz % 8 == 0); // so we our 4-elem writes always fit in 8B
    static constexpr int elems_per_u64 = sizeof(uint64_t) / sizeof(uint_t);
    int u64_chunks_per_block = BlockSz / elems_per_u64;

    if (length <= 0) { return length; }

    auto orig_data_out = data_out;

    len_t nblocks = length / BlockSz;

    // nblocks = 0; // all tests pass with this in both enc and dec
    // nblocks -= 1;

    len_t full_blocks_len = nblocks * BlockSz;
    len_t tail_len = length - full_blocks_len;

    uint_t tmp0[BlockSz];

    // printf("enc tail len: %d\n", tail_len);

    // printf("enc last 16 values: "); dump_elements(data_in + length - 16, 16);


    // printf("enc out ptr: %p\n", data_out);

    // zero out headers buffer; it's a bitfield so length/8 bytes, rounded up
    // memset(headers_out, 0, spritzpack_headers_size_bytes_u16(length));
    // for (int i = 0; i < header_nbytes; i++) { headers_out[i] = 0; }

    int max_payload_nbytes = sizeof(uint_t) * BlockSz;
//    printf("max_payload_nbytes: %d\n", max_payload_nbytes);
    if (nblocks >= 1) {
        memset(data_out, 0, max_payload_nbytes);
    }

    uint8_t shift = 0;  // for writing non-byte-aligned data
    for (len_t b = 0; b < nblocks; b++) {
        // determine number of bits needed for this block
        for (int bb = 0; bb < BlockSz; bb++) {
            uint_t val = *data_in++;
            if (ZigZag) {
                tmp0[bb] = zigzag_encode_16b(val);
            } else {
                tmp0[bb] = val;
            }
        }
        uint8_t nbits = needed_nbits_u16x8_simple(tmp0);

        // if (b == nblocks - 1) { nbits = 16; } // TODO rm
        // printf("enc nbits: %d\n", nbits);
        // nbits = 16; // TODO rm after debug
        // nbits = 12; // TODO rm after debug
        auto write_nbits = nbits - (nbits == 16);


        // write out header
        static_assert(nbits_sz_bits == 4, "impl assumes 4bit headers");
        if (b % 2) {
            *headers_out |= write_nbits << 4;
            headers_out++;
        } else {
            *headers_out = write_nbits;
        }

        // yep, this is correct
        // printf("tmp0: "); dump_elements(tmp0, BlockSz);

        // write out bitpacked payload
        // cross-platform impl; pdep approach is much faster on x86
        // NOTE: this does assume little-endian byte order
        uint_t mask = (uint_t)((1 << nbits) - 1);
//        printf("mask: %d\n", mask);
        // assert(mask == 0xffff); // TODO rm
        for (int c = 0; c < u64_chunks_per_block; c++) {
            uint64_t packed = 0;
            // pack groups of elems into u64s so we only have to do one
            // unaligned write (and update of output shift / ptr)
            uint8_t intra_u64_shift = 0;
            for (int cc = 0; cc < elems_per_u64; cc++) {
                auto idx = c * elems_per_u64 + cc;
                // uint_t val = tmp0[idx] & mask; // not needed by def of nbits
                uint64_t val = tmp0[idx];
                // printf("intra_u64_shift: %d\n", intra_u64_shift);
                packed |= val << intra_u64_shift;
                intra_u64_shift += nbits;
                // printf("packed: "); dump_elements((uint16_t*)&packed, 4);
            }

            // write out packed data, then update write ptr and shift
            // it's actually pretty subtle why an 8B write always works
            // here; requires that 1) blocks end byte aligned (which happens
            // automatically with (blocksz % 8 == 0), and 2) 15b isn't allowed;
            // this way prev chunk of 4 is always either byte aligned (which
            // happens with even nbits), or this chunk takes at most 13*4=52b,
            // so trailing 4b from previous writes are okay
            // printf("crap in data out: "); dump_elements(data_out, 4);
            *((uint64_t*)data_out) |= packed << shift;
            uint8_t nbits_written = nbits * elems_per_u64;
            shift += nbits_written;
            data_out = (int_t*)(((uint8_t*)data_out) + shift / 8);
            shift = shift % 8;
        }

        // zero out next chunk of output buffer; this is always safe if
        // we've been passed a large enough output buffer (same length as
        // input or more)
        if (b < nblocks - 1) {
            memset(data_out, 0, max_payload_nbytes);
        }
    }

    // handle trailing elems; note that end of block always byte aligned
    // if (shift > 0) { data_out++; }  // just ignore trailing bits
    for (len_t i = 0; i < tail_len; i++) {
        *data_out++ = *data_in++;
    }

    // printf("enc wrote encoded elems: "); dump_elements((uint16_t*)data_out - length, length);
    // printf("enc wrote encoded elems: "); dump_elements((uint16_t*)data_out - 8, 8);
    // printf("enc wrote encoded elems: "); dump_elements((uint16_t*)data_out - 12, 12);
    // printf("enc out ptr: %p\n", data_out - length);
    // printf("enc wrote header: "); //dump_elements(headers_out - headers_len, headers_len);
    // for (int i = 0; i < nblocks; i++) {
    //     printf("%d ", (headers_out[i / 2] >> (i % 2 ? 4 : 0)) & 0xf);
    // }
    // printf("\n");
    auto ret_nbytes = ((uint8_t*)data_out) - ((uint8_t*)orig_data_out);
    auto ret = (len_t)(ret_nbytes + 1) / 2;
    if (ret_nbytes % 2) {
        // if didn't consume an even number of u16s, ensure that the trailing
        // byte is deterministic (probably unnecessary by why take chances)
        ((uint8_t*)orig_data_out)[ret_nbytes + 1] = 0;
    }

    // auto ret = (len_t)(data_out - orig_data_out);
    assert(ret <= length);
    // printf("enc returning len %d\n", ret);
    // auto byteptr = ((uint8_t*)orig_data_out) + ret_nbytes;
    // printf("enc encoded elems v2: "); dump_elements(((uint16_t*)byteptr) - 12, 12);
    return ret;

    // return length;
}


// template<int BlockSz=8, bool ZigZag=false>
template<bool ZigZag=true, int BlockSz=8>
len_t _sprintzpack_decode_u16(
    const int16_t* data_in, len_t length, uint16_t* data_out,
    const uint8_t* headers_in)
{
    typedef int16_t int_t;
    typedef uint16_t uint_t;
    static constexpr int nbits_sz_bits = 4;
    static_assert(nbits_sz_bits == 4, "impl assumes 4bit headers");
    assert (BlockSz % 4 == 0); // our impl unpacks 4 elem chunks into uint64s
    static constexpr int elems_per_u64 = sizeof(uint64_t) / sizeof(uint_t);
    int u64_chunks_per_block = BlockSz / elems_per_u64;

    if (length <= 0) { return length; }

    len_t nblocks = length / BlockSz;

    // nblocks -= 1; // TODO rm


    len_t full_blocks_len = nblocks * BlockSz;
    len_t tail_len = length - full_blocks_len;

    // printf("dec in ptr: %p\n", data_in);
    // printf("dec sees length: %d\n", length);
    // printf("dec sees initial encoded elems: "); dump_elements((uint16_t*)data_in, 8);
    // printf("dec sees encoded elems: "); dump_elements((uint16_t*)data_in, length);
    // printf("dec sees header: ");
    // printf("dec tail len: %d\n", tail_len);

    // printf("dec last 16 inputs: "); dump_elements(data_in + length - 16, 16);

    // double nbits_sum = 0;
    // for (int i = 0; i < nblocks; i++) {
    //     auto nbits = (headers_in[i / 2] >> (i % 2 ? 4 : 0)) & 0xf;
    //     nbits_sum += nbits;
    //     // printf("%d ", (headers_in[i / 2] >> (i % 2 ? 4 : 0)) & 0xf);
    // }
    // printf("\n");
    // if (nblocks >= 1) {
    //     printf("dec mean nbits: %g\n", nbits_sum / nblocks);
    // }

    // printf("dec sees encoded 8B before memset: "); dump_elements((uint16_t*)data_in, length);

    auto block_payload_nbytes = BlockSz * sizeof(uint_t);
    if (nblocks >= 1) {
        memset(data_out, 0, block_payload_nbytes);
    }

    // printf("dec sees encoded 8B after memset: "); dump_elements((uint16_t*)data_in, length);

    uint8_t shift = 0;  // for reading non-byte-aligned data
    for (len_t b = 0; b < nblocks; b++) {
        // read header
        uint8_t nbits;
        if (b % 2) {
            nbits = ((*headers_in++) >> 4) & 0x0F;
        } else {
            nbits = (*headers_in) & 0x0F;
        }
        nbits += (nbits == 15); // 15 is actually 16, since 16 doesn't fit


        // nbits = 12; // TODO rm after debug


        uint64_t mask = (((uint64_t)1) << nbits) - 1;
        // printf("nbits: %d\n", nbits);
        // printf("mask: %llu\n", mask);

        // read payload
        for (int c = 0; c < u64_chunks_per_block; c++) {
            auto in_ptr = (const uint64_t*)data_in;
            uint64_t packed = (*in_ptr) >> shift;


            // printf("dec sees encoded 8B: "); dump_elements((uint16_t*)in_ptr, 4);
            // printf("packed: "); dump_elements((uint16_t*)&packed, 4);


            for (int cc = 0; cc < elems_per_u64; cc++) {
                uint8_t rshift = cc * nbits;
                uint_t val = (uint_t)((packed >> rshift) & mask);
                // printf("val: %d\n", val);
                *data_out++ = ZigZag ? zigzag_decode_16b(val) : val;
            }
            uint8_t nbits_written = nbits * elems_per_u64;
            shift += nbits_written;
            // data_in += shift / 8;
            data_in = (int_t*)(((uint8_t*)data_in) + shift / 8);
            shift = shift % 8;
        }
    }

    // handle trailing elems; note that end of block always byte aligned
    for (len_t i = 0; i < tail_len; i++) {
        // printf("copying tail value %d\n", *data_in);
        *data_out++ = *data_in++;
    }
    // printf("dec last 8 outputs: "); dump_elements(data_out - 8, 8);
    // printf("dec last 16 outputs: "); dump_elements(data_out - 16, 16);
    // printf("dec last 12 outputs: "); dump_elements(data_out - 12, 1);
    // printf("dec returning length: %d\n", length);


    return length;
}

len_t sprintzpack_encode_u16(
    const uint16_t* data_in, len_t length, int16_t* data_out,
    uint8_t* headers_out, bool zigzag)
{
    // zigzag = true; // TODO rm
    if (zigzag) {
        return _sprintzpack_encode_u16<true>(
            data_in, length, data_out, headers_out);
    } else {
        return _sprintzpack_encode_u16<false>(
            data_in, length, data_out, headers_out);
    }
}
len_t sprintzpack_decode_u16(
    const int16_t* data_in, len_t length, uint16_t* data_out,
    const uint8_t* headers_in, bool zigzag)
{
    // zigzag = true; // TODO rm
    if (zigzag) {
        return _sprintzpack_decode_u16<true>(
            data_in, length, data_out, headers_in);
    } else {
        return _sprintzpack_decode_u16<false>(
            data_in, length, data_out, headers_in);
    }
}

len_t _sprintzpack_pack_u16(
    const uint16_t* data_in, size_t size, int16_t* data_out, bool zigzag)
{
    len_t length = (len_t)size; // avoid implicit conversion warnings
    uint16_t offset = write_metadata_simple1d(data_out, length);
    data_out += offset;

    // uint8_t* headers_out = (uint8_t*)(data_out + length);
    len_t headers_size = (sprintzpack_headers_size_bytes_u16(length) + 1) / 2;
    uint8_t* headers_out = (uint8_t*)(data_out);
    // int16_t* payloads_out = (int16_t*)((uint8_t*)data_out + headers_size);
    int16_t* payloads_out = data_out + headers_size;
    auto len = sprintzpack_encode_u16(
        data_in, length, payloads_out, headers_out, zigzag);
    return offset + headers_size + len;
}
len_t sprintzpack_pack_u16(
    const uint16_t* data_in, size_t length, int16_t* data_out)
{
    return _sprintzpack_pack_u16(data_in, length, data_out, false);
}
len_t sprintzpack_pack_u16_zigzag(
    const uint16_t* data_in, size_t length, int16_t* data_out)
{
    return _sprintzpack_pack_u16(data_in, length, data_out, true);
}

len_t _sprintzpack_unpack_u16(
    const int16_t* data_in, uint16_t* data_out, bool zigzag)
{
    len_t length;
    uint16_t offset = read_metadata_simple1d(data_in, &length);
    data_in += offset;
    // const uint8_t* headers_in = (const uint8_t*)(data_in + length);
    len_t headers_size = (sprintzpack_headers_size_bytes_u16(length) + 1) / 2;
    const uint8_t* headers_in = (const uint8_t*)data_in;
    const int16_t* payloads_in = data_in + headers_size;
    // return sprintzpack_decode_u16(payloads_in, length, data_out, headers_in);
    return sprintzpack_decode_u16(
            payloads_in, length, data_out, headers_in, zigzag);
}

len_t sprintzpack_unpack_u16(const int16_t* data_in, uint16_t* data_out) {
    return _sprintzpack_unpack_u16(data_in, data_out, false);
}

len_t sprintzpack_unpack_u16_zigzag(const int16_t* data_in, uint16_t* data_out)
{
    return _sprintzpack_unpack_u16(data_in, data_out, true);
}
