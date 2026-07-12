#pragma once

#include <vector>
#include <string>
#include <cstring>
#include <type_traits>
#include <openssl/hmac.h>
#include <openssl/evp.h>

inline thread_local const std::string* tl_secretKey = nullptr;

// ============================================================================
// 1. FEISTEL NETWORK CORE (Format-Preserving Integer Encryption)
// ============================================================================

// Internal helper function (HMAC-based round function)
inline uint64_t feistelRoundFn(uint64_t r, uint8_t round, int halfBytes)
{
    const std::string* keyPtr = tl_secretKey;
    uint8_t input[9]; // max 8 bytes
    std::memcpy(input, &r, static_cast<size_t>(halfBytes));
    input[halfBytes] = round;

    unsigned char hmacOut[EVP_MAX_MD_SIZE];
    unsigned int hmacLen = 0;
    HMAC(
        EVP_sha256(),
        keyPtr->data(),
        static_cast<int>(keyPtr->size()),
        input,
        halfBytes + 1,
        hmacOut,
        &hmacLen
    );

    uint64_t result = 0;
    std::memcpy(&result, hmacOut, static_cast<size_t>(halfBytes));
    return result;
}


// ---------------------------------------------------------
// Pseudonymize (Integer)
// ---------------------------------------------------------
namespace
{
template <typename T>
T feistelPseudonymization(T inputId)
{
    static_assert(std::is_integral_v<T> && std::is_signed_v<T>);
    using U = std::make_unsigned_t<T>;

    constexpr int HALF_BITS = (sizeof(T) == 1) ? 4
                            : (sizeof(T) == 2) ? 8
                            : (sizeof(T) == 4) ? 16 : 32;

    constexpr int HALF_BYTES = (sizeof(T) <= 2) ? 1
                             : (sizeof(T) == 4) ? 2 : 4;

    constexpr U HALF_MASK = (sizeof(T) == 1) ? static_cast<U>(0x0F)
    : (sizeof(T) == 2) ? static_cast<U>(0xFF)
    : (sizeof(T) == 4) ? static_cast<U>(0xFFFF)
    :                    static_cast<U>(0xFFFFFFFF);

    const U val = static_cast<U>(inputId);
    U L = static_cast<U>(val >> HALF_BITS) & HALF_MASK;
    U R = val & HALF_MASK;

    for (uint8_t i = 0; i < 4; ++i)
    {
        const U fr = static_cast<U>(
                        feistelRoundFn(static_cast<uint64_t>(R), i, HALF_BYTES)
                     ) & HALF_MASK;
        const U newR = (L ^ fr) & HALF_MASK;
        L = R;
        R = newR;
    }

    return static_cast<T>((static_cast<U>(L) << HALF_BITS) | R);
}
}


// ---------------------------------------------------------
// De-Pseudonymize (Integer)
// ---------------------------------------------------------
namespace
{
template <typename T>
T feistelDepseudonymization(T inputId)
{
    static_assert(std::is_integral_v<T> && std::is_signed_v<T>);
    using U = std::make_unsigned_t<T>;

    constexpr int HALF_BITS  = (sizeof(T) == 1) ?  4
                             : (sizeof(T) == 2) ?  8
                             : (sizeof(T) == 4) ? 16 : 32;

    constexpr int HALF_BYTES = (sizeof(T) <= 2) ? 1
                             : (sizeof(T) == 4) ? 2 : 4;

    constexpr U HALF_MASK = (sizeof(T) == 1) ? static_cast<U>(0x0F)
    : (sizeof(T) == 2) ? static_cast<U>(0xFF)
    : (sizeof(T) == 4) ? static_cast<U>(0xFFFF)
    :                    static_cast<U>(0xFFFFFFFF);

    const U val = static_cast<U>(inputId);
    U L = static_cast<U>(val >> HALF_BITS) & HALF_MASK;
    U R = val & HALF_MASK;

    for (int n = 3; n >= 0; --n)
    {
        const U fL    = static_cast<U>(
                            feistelRoundFn(static_cast<uint64_t>(L), static_cast<uint8_t>(n), HALF_BYTES)
                        ) & HALF_MASK;
        const U prevL = (R ^ fL) & HALF_MASK;
        const U prevR = L;
        L = prevL;
        R = prevR;
    }

    return static_cast<T>((static_cast<U>(L) << HALF_BITS) | R);
}
}



// ============================================================================
// 2. STRING (VARSIZED) ENCRYPTION (AES-256-CTR)
// ============================================================================

// Internal helper functions
inline void deriveAESKey(unsigned char* outKey32)
{
    const std::string* keyPtr = tl_secretKey;
    unsigned int mdLen = 0;
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    EVP_DigestUpdate(ctx, keyPtr->data(), static_cast<int>(keyPtr->size()));
    EVP_DigestFinal_ex(ctx, outKey32, &mdLen);
    EVP_MD_CTX_free(ctx);
}

inline void hexEncode(const unsigned char* src, int srcLen, int8_t* dst)
{
    static const char hexChars[] = "0123456789abcdef";
    for (int i = 0; i < srcLen; ++i)
    {
        dst[i * 2]     = static_cast<int8_t>(hexChars[(src[i] >> 4) & 0xF]);
        dst[i * 2 + 1] = static_cast<int8_t>(hexChars[src[i] & 0xF]);
    }
}

inline void hexDecode(const int8_t* src, int srcLen, unsigned char* dst)
{
    auto nibble = [](char c) -> unsigned char {
        if (c >= '0' && c <= '9') return static_cast<unsigned char>(c - '0');
        if (c >= 'a' && c <= 'f') return static_cast<unsigned char>(c - 'a' + 10);
        return static_cast<unsigned char>(c - 'A' + 10);
    };
    for (int i = 0; i < srcLen / 2; ++i)
        dst[i] = static_cast<unsigned char>(
            (nibble(static_cast<char>(src[i * 2])) << 4) |
             nibble(static_cast<char>(src[i * 2 + 1])));
}


// ---------------------------------------------------------
// Pseudonymize (String)
// ---------------------------------------------------------
inline uint64_t pseudonymizeString(int8_t* inputPtr, uint64_t inputSize, int8_t* outputPtr)
{
    const std::string* keyPtr = tl_secretKey;
    unsigned char aesKey[32];
    deriveAESKey(aesKey);

    unsigned char hmacOut[EVP_MAX_MD_SIZE];
    unsigned int  hmacLen = 0;
    HMAC(EVP_sha256(),
         keyPtr->data(), static_cast<int>(keyPtr->size()),
         reinterpret_cast<const unsigned char*>(inputPtr), static_cast<int>(inputSize),
         hmacOut, &hmacLen);

    unsigned char iv[16];
    std::memcpy(iv, hmacOut, 16);

    std::vector<unsigned char> ct(static_cast<size_t>(inputSize));
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    int outLen = 0, finalLen = 0;
    EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, aesKey, iv);
    EVP_EncryptUpdate(ctx, ct.data(), &outLen,
                      reinterpret_cast<const unsigned char*>(inputPtr),
                      static_cast<int>(inputSize));
    EVP_EncryptFinal_ex(ctx, ct.data() + outLen, &finalLen);
    EVP_CIPHER_CTX_free(ctx);

    hexEncode(iv, 16, outputPtr);
    hexEncode(ct.data(), static_cast<int>(inputSize), outputPtr + 32);

    return static_cast<uint64_t>(32) + inputSize * static_cast<uint64_t>(2);
}


// ---------------------------------------------------------
// De-Pseudonymize (String)
// ---------------------------------------------------------
inline uint64_t depseudonymizeString(int8_t* inputPtr, uint64_t inputSize, int8_t* outputPtr)
{
    unsigned char aesKey[32];
    deriveAESKey(aesKey);

    unsigned char iv[16];
    hexDecode(inputPtr, 32, iv);

    const uint64_t ctHexLen  = inputSize - static_cast<uint64_t>(32);
    const uint64_t ctByteLen = ctHexLen / static_cast<uint64_t>(2);
    std::vector<unsigned char> ct(static_cast<size_t>(ctByteLen));
    hexDecode(inputPtr + 32, static_cast<int>(ctHexLen), ct.data());

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    int outLen = 0, finalLen = 0;
    EVP_DecryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr, aesKey, iv);
    EVP_DecryptUpdate(ctx, reinterpret_cast<unsigned char*>(outputPtr), &outLen,
                      ct.data(), static_cast<int>(ctByteLen));
    EVP_DecryptFinal_ex(ctx, reinterpret_cast<unsigned char*>(outputPtr) + outLen, &finalLen);
    EVP_CIPHER_CTX_free(ctx);

    return ctByteLen;
}



