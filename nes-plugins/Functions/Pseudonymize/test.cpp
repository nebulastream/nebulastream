



namespace detail {
// Shared Secret Key (thread_local resolves Nautilus JIT constraints)
inline thread_local const std::string* tl_secretKey = nullptr;

// 1. Format-Preserving Encryption for Integers (Feistel Network)
template <typename T>
T feistelPseudonymization(T input) { /* ... */ }

template <typename T>
T feistelDepseudonymization(T input) { /* ... */ }

// 2. Stream Cipher for Variable Strings (AES-256-CTR)
inline uint64_t pseudonymizeString(int8_t* state, uint32_t length) { /* ... */ }

inline uint64_t depseudonymizeString(int8_t* state, uint32_t length) { /* ... */ }
}


