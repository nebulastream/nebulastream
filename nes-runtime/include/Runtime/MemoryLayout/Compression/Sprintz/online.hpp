//
//  online.hpp
//  Compress
//
//  Created by DB on 3/31/20.
//  Copyright © 2020 D Blalock. All rights reserved.
//

#ifndef online_h
#define online_h

#include <stdint.h>

#include "bitpack.h" // for nbits cost arrays
#include "macros.h"  // just for restrict

// NOTE: a lot of this could be cleaner / more robust with more cpp features
// (e.g., checking that Predictor scalar type matches OnlineCoder scalar type),
// but we're writing it such that it will be easy to port to C

// ================================================================ types

// handle buffers longer than 2^32; signed so differences work correctly
// typedef int64_t idx_t;

typedef uint32_t len_t;

// =============================================================== helper funcs

// inline uint8_t _add_with_wraparound_i8(uint8_t a, int8_t b) {
//     // return a + b;  // technically undefined behavior, but usually works

//     // force 2s complement add
//     #ifdef __arm__
//         uint8_t ret;
//         asm("add %[c], %[a], %[b]" : [c] "=g" (ret) : [a] "g" (a), [b] "g" (b));
//         return ret;
//     #else  // assumes x86
//         asm("add %[a], %[b]" : [a] "+%g" (a) : [b] "g" (b));
//         return a;
//     #endif
// }

// inline int8_t _sub_with_wraparound_i8(uint8_t a, uint8_t b) {
//     // force 2s complement add
//     #ifdef __arm__
//         uint8_t ret;
//         asm("sub %[c], %[a], %[b]" : [c] "=g" (ret) : [a] "g" (a), [b] "g" (b));
//         return ret;
//     #else  // assumes x86
//         asm("sub %[a], %[b]" : [a] "+%g" (a) : [b] "g" (b));
//         return a;
//     #endif
// }

// inline uint16_t _add_with_wraparound_i16(uint16_t a, int16_t b) {
//     // return a + b;  // technically undefined behavior, but usually works

//     // force 2s complement add
//     #ifdef __arm__
//         uint16_t ret;
//         asm("add %[c], %[a], %[b]" : [c] "=g" (ret) : [a] "g" (a), [b] "g" (b));
//         return ret;
//     #else  // assumes x86
//         asm("add %[a], %[b]" : [a] "+%g" (a) : [b] "g" (b));
//         return a;
//     #endif
// }

// inline int16_t _sub_with_wraparound_i16(uint16_t a, int16_t b) {
//     // force 2s complement sub
//     #ifdef __arm__
//         int16_t ret;
//         asm("sub %[c], %[a], %[b]" : [c] "=g" (ret) : [a] "g" (a), [b] "g" (b));
//         return ret;
//     #else  // assumes x86
//         asm("sub %[a], %[b]" : [a] "+%g" (a) : [b] "g" (b));
//         return a;
//     #endif
// }

inline uint16_t _add_with_wraparound(uint16_t a, int16_t b) {
    // return a + b;  // technically undefined behavior, but usually works

    // force 2s complement add
    #ifdef __arm__
        uint16_t ret;
        // asm("add %[c], %[a], %[b]" : [c] "=g" (ret) : [a] "g" (a), [b] "g" (b));
        asm("add %[a], %[b], %[c]" : [c] "=g" (ret) : [a] "g" (a), [b] "g" (b));
        return ret;
    #else  // assumes x86
        // asm("add %[a], %[b]" : [a] "+g" (a) : [b] "g" (b));
        // asm("add %[b], %[a]" : [a] "+%g" (a) : [b] "g" (b));
        asm("add %[a], %[b]" : [b] "+g" (b) : [a] "g" (a));
        // return a;
        return b;
    #endif
}

inline int16_t _sub_with_wraparound(uint16_t a, int16_t b) {
    // force 2s complement sub
    #ifdef __arm__
        int16_t ret;
        // asm("sub %[c], %[a], %[b]" : [c] "=g" (ret) : [a] "g" (a), [b] "g" (b));
        asm("sub %[a], %[b], %[c]" : [c] "=g" (ret) : [a] "g" (a), [b] "g" (b));
        return ret;
    #else  // assumes x86
        // asm("sub %[a], %[b]" : [a] "+%g" (a) : [b] "g" (b));
        // return a;
        asm("sub %[b], %[a]" : [a] "+%g" (a) : [b] "g" (b));
        return a;
    #endif
}


// =========================================== predictive coding state machines

struct DeltaPredictor_u16 {
    using data_t = uint16_t;
    using err_t = int16_t;

    void init(data_t initial_val) {
        _prev = initial_val;
    }

    void jump(data_t prev0, data_t prev1, data_t prev2) {
        _prev = prev0;
    }

    data_t predict() const {
        // printf("delta prev_val: %d\n", _prev);
        return _prev;
    }

    void train(err_t err, data_t true_val) {
        _prev = true_val;
    }

private:
    data_t _prev;
};

struct DoubleDeltaPredictor_u16 {
    using data_t = uint16_t;
    using err_t = int16_t;

    // TODO implement method to let it skip to end of block for dynamic
    // predictor encoding
    // void full_init(data_t prev0, data_t prev1, data_t prev2, data_t prev3) {
    //     _prev0 = prev0;
    //     _prev1 = prev1;
    // }

    void init(data_t initial_val) {
        // full_init(initial_val, initial_val, 0, 0);
        _prev_val = initial_val;
        _prev_diff = 0;
        // _prev1 = initial_val; // make it equivalent to delta coding at first
    }

    void jump(data_t prev0, data_t prev1, data_t prev2) {
        _prev_val = prev0;
        _prev_diff = _sub_with_wraparound(prev0, prev1);
    }

    data_t predict() const {
        // like `_prev + (prev0 - prev1)`, but avoids undefined behavior
        // err_t diff = _sub_with_wraparound(_prev0, _prev1);
        // return _add_with_wraparound(_prev0, diff);
        // printf("double delta prev_val: %d\n", _prev_val);
        return _add_with_wraparound(_prev_val, _prev_diff);
    }

    void train(err_t err, data_t true_val) {
        _prev_diff = _sub_with_wraparound(true_val, _prev_val);
        _prev_val = true_val;
        // _prev1 = _prev0;
        // _prev0 = true_val;
    }

private: // TODO uncomment
    // data_t _prev0;
    // data_t _prev1;
    data_t _prev_val;
    err_t _prev_diff;
};


struct TripleDeltaPredictor_u16 {
    using data_t = uint16_t;
    using err_t = int16_t;

    void init(data_t initial_val) {
        _prev_val = initial_val;
        // make it equivalent to delta coding at first
        _prev_diff = 0;
        _prev_ddiff = 0;

        // _prev0 = initial_val;
        // // make it equivalent to delta coding at first
        // _prev1 = initial_val;
        // _prev2 = initial_val;
    }

    void jump(data_t prev0, data_t prev1, data_t prev2) {
        _prev_val = prev0;
        _prev_diff = _sub_with_wraparound(prev0, prev1);
        err_t diff1 = _sub_with_wraparound(prev1, prev2);
        _prev_ddiff = _sub_with_wraparound(_prev_diff, diff1);
    }

    data_t predict() const {
        err_t predicted_diff = _add_with_wraparound(_prev_diff, _prev_ddiff);
        // printf("trip prev val:   %d\n", _prev_val);
        // printf("trip prev diff:  %d\n", _prev_diff);
        // printf("trip prev ddiff: %d\n", _prev_ddiff);
        // printf("trip predicted diff: %d\n", predicted_diff);
        return _add_with_wraparound(_prev_val, predicted_diff);

        // err_t diff0 = _sub_with_wraparound(_prev0, _prev1);
        // printf("trip diff0: %d\n", diff0);
        // err_t diff1 = _sub_with_wraparound(_prev1, _prev0);
        // printf("trip diff1: %d\n", diff1);
        // err_t ddiff = _sub_with_wraparound(diff0, diff1);
        // printf("trip ddiff: %d\n", ddiff);
        // data_t linear_prediciton = _add_with_wraparound(_prev0, diff0);
        // printf("trip linpred: %d\n", linear_prediciton);
        // return _add_with_wraparound(linear_prediciton, ddiff);
    }

    void train(err_t err, data_t true_val) {
        err_t diff = _sub_with_wraparound(true_val, _prev_val);
        _prev_ddiff = _sub_with_wraparound(diff, _prev_diff);
        _prev_diff = diff;
        _prev_val = true_val;

        // _prev2 = _prev1;
        // _prev1 = _prev0;
        // _prev0 = true_val;
    }

private: // TODO uncomment
    // data_t _prev0;
    // data_t _prev1;
    // data_t _prev2;
    data_t _prev_val;
    err_t _prev_diff;
    err_t _prev_ddiff;
};

struct MovingAvgPredictor_u16 {
    using data_t = uint16_t;
    using err_t = int16_t;

    static const uint8_t _shift = 2;

    void init(data_t initial_val) {
        _accumulator = initial_val << _shift;
    }

    void jump(data_t prev0, data_t prev1, data_t prev2) {
        assert(false); // finite history invalid for IIR filter
    }

    data_t predict() const {
        return _accumulator >> _shift;
    }

    void train(err_t err, data_t true_val) {
        // this implements a moving average in a nice way that uses extremely
        // few instructions and also keeps the low bits; derivation:
        //
        // a(t+1) = .25 * x(t) + .75 * a(t)
        // 4a(t+1) = x(t) + 3a(t)
        // 4a(t+1) = a(t) + err(t) + 3a(t)
        // a(t+1) = a(t) + .25 * err(t)
        //
        // and of course, could use different shift amounts to get different
        // convex combinations
        _accumulator += err;
    }

private:
    int32_t _accumulator;
};

template<class predictor_type>
struct PredictiveCoder {
    using data_t = typename predictor_type::data_t;
    using err_t = typename predictor_type::err_t;

    void init(data_t initial_val) {
        _predictor.init(initial_val);
    }

    // lets you skip to somewhere else in a buffer without having to
    // feed in all the intermediate values; note that some predictors
    // might not actually let you do this; in particular, moving avg
    void jump(data_t prev0, data_t prev1, data_t prev2) {
        _predictor.jump(prev0, prev1, prev2);
    }

    err_t encode_next(data_t val) {
        data_t prediction = _predictor.predict();
        // printf("enc prediction: %d\n", prediction);
        // err_t err = val - prediction;
        err_t err = _sub_with_wraparound(val, prediction);
        // printf("enc err: %d\n", err);
        // printf("---- enc val: %d\n", val);
        _predictor.train(err, val);
        // if (val == 81) {
        //     printf("val = %d, prediction = %d, err = %d\n", val, prediction, err);
        // }
        return err;
    }

    data_t decode_next(err_t err) {
        data_t prediction = _predictor.predict();
        // printf("dec prediction: %d\n", prediction);
        data_t val = (data_t)_add_with_wraparound(prediction, err);
        // data_t val = prediction + err;
        // printf("dec err: %d\n", err);
        // printf("dec val: %d\n", val);
        // data_t val = prediction + err;
        // printf("dec err, prediction, val = %d, %d, %d\n", err, prediction, val);
        _predictor.train(err, val);
        return val;
    }

    // this is needed so that you can feed in data to keep the state
    // correct even when using a different codec (when using a dynamically
    // chosen predictor)
    void train(data_t true_val) {
        data_t prediction = _predictor.predict();
        err_t err = true_val - prediction;
        _predictor.train(err, true_val);
    }

// private: // TODO uncomment after debug
    predictor_type _predictor;
};

template<class predictor_type, class int_t0, class int_t1>
len_t _predictive_code(const int_t0* data_in, int_t1* data_out,
                      len_t length, bool encode)
{
    if (length <= 0) { return length; }
    // always copy the first value
    *data_out = *data_in;
    if (length == 1) { return length; }

    // create predictive coder and either call encode or decode on each scalar
    PredictiveCoder<predictor_type> coder;
    coder.init(data_in[0]);
    if (encode) {
        for (uint32_t i = 1; i < length; i++) {
            data_out[i] = coder.encode_next(data_in[i]);
            // printf("i = %d, enc wrote val: %d\n", i, data_out[i]);
        }
    } else {
        for (uint32_t i = 1; i < length; i++) {
            // data_out[i] = coder.decode_next(data_in[i]);
            auto tmp = coder.decode_next(data_in[i]);
            data_out[i] = tmp;
            // printf("decode next returned: %d\n", tmp);
            // printf("i = %d, dec wrote val: %d\n", i, data_out[i]);
        }
    }
    return length;
}

template<class predictor_type, class uint_t, class int_t>
len_t predictive_encode(const uint_t* input, len_t length, int_t* output) {
    return _predictive_code<predictor_type>(
        input, output, length, true /* encode */);
}

template<class predictor_type, class uint_t, class int_t>
len_t predictive_decode(const uint_t* input, len_t length, int_t* output) {
    return _predictive_code<predictor_type>(
        input, output, length, false /* decode */);
}

// =================================================== dynamic predictor choice

typedef int32_t loss_t;
// static const int kMinPossibleLoss = -(1 << (8 * sizeof(loss_t) - 1));

struct Losses {  // just wrapping the enum so it has some reasonable scoping
    enum { MaxAbs, SumLogAbs };
};



len_t dynamic_delta_choices_size_bytes(len_t length, int blocksz=8);

len_t dynamic_delta_zigzag_encode_u16(
    const uint16_t* data_in, len_t length, int16_t* data_out,
    uint8_t* choices_out, int loss=Losses::SumLogAbs);
// this version writes out choices buff at end of data_out and stores length;
// goes with decomp func below that doesn't take in length or choices buff
len_t dynamic_delta_pack_u16(
    const uint16_t* data_in, size_t length, int16_t* data_out);
len_t dynamic_delta_pack_u16_altloss(
    const uint16_t* data_in, size_t length, int16_t* data_out);

len_t dynamic_delta_zigzag_decode_u16(
    const int16_t* data_in, len_t length, uint16_t* data_out,
    const uint8_t* choices_in);
len_t dynamic_delta_unpack_u16(
    const int16_t* data_in, uint16_t* data_out);

// =================================================== just zigzag
// TODO this isn't really "online"

len_t zigzag_encode_u16(
    const uint16_t* data_in, len_t length, int16_t* data_out);
len_t zigzag_pack_u16(
    const uint16_t* data_in, size_t length, int16_t* data_out);

len_t zigzag_decode_u16(
    const int16_t* data_in, len_t length, uint16_t* data_out);
len_t zigzag_unpack_u16(
    const int16_t* data_in, uint16_t* data_out);

// =================================================== just sprintz bitpack
// TODO this isn't really "online"

len_t sprintzpack_headers_size_bytes_u16(len_t length, int blocksz=8);

len_t sprintzpack_encode_u16(
    const uint16_t* data_in, len_t length, int16_t* data_out,
    uint8_t* headers_out, bool zigzag=true);
// this version writes out choices buff at end of data_out and stores length;
// goes with decomp func below that doesn't take in length or choices buff
len_t sprintzpack_pack_u16(
    const uint16_t* data_in, size_t length, int16_t* data_out);
len_t sprintzpack_pack_u16_zigzag(
    const uint16_t* data_in, size_t length, int16_t* data_out);
    // bool zigzag=true);

len_t sprintzpack_decode_u16(
    const int16_t* data_in, len_t length, uint16_t* data_out,
    const uint8_t* headers_in, bool zigzag=true);
// TODO don't require passing in of zigzag here; enc should store this param
len_t sprintzpack_unpack_u16(const int16_t* data_in, uint16_t* data_out);
len_t sprintzpack_unpack_u16_zigzag(const int16_t* data_in, uint16_t* data_out);

#endif /* online_h */
