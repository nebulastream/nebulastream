# NebulaStream Custom Functions: Pseudonymization & Depseudonymization

This directory contains the native in-stream pseudonymization and depseudonymization operator plugins developed for the NebulaStream ecosystem. These operators provide real-time data protection for sensitive medical records (e.g., patient IDs and string attributes) directly within the stream processing engine without requiring redundant clone databases.

---

## 📁 Directory Structure

```text
Functions/
├── Pseudonymize/
│   ├── FeistelCipher.hpp                # Core cryptographic primitives (Feistel FPE & AES-256-CTR)
│   ├── PseudonymizeLogicalFunction.hpp    # Logical operator definition
│   ├── PseudonymizeLogicalFunction.cpp    # Logical operator implementation
│   ├── PseudonymizePhysicalFunction.hpp   # Physical operator definition
│   ├── PseudonymizePhysicalFunction.cpp   # Physical execution & Nautilus JIT dispatch
│   └── CMakeLists.txt                     # Build and plugin registration configuration
└── Depseudonymize/
    ├── DepseudonymizeLogicalFunction.hpp  # Logical operator definition
    ├── DepseudonymizeLogicalFunction.cpp  # Logical operator implementation
    ├── DepseudonymizePhysicalFunction.hpp # Physical operator definition
    ├── DepseudonymizePhysicalFunction.cpp # Physical execution & inverse JIT dispatch
    └── CMakeLists.txt                     # Build and plugin registration configuration
```

--- 

### 🔒 Cryptographic Mechanisms

* **Integer Pseudonymization** (*Format-Preserving Encryption - FPE* ):
  * Implements a **4-round Feistel network** utilizing an HMAC-SHA256 round function.
  * Supports exact-width signed integers (`INT8`, `INT16`, `INT32`, `INT64`) while strictly preserving their bit-length and data type to prevent downstream application breakage.
  * Dynamically varies round iterations by appending the round index to the HMAC input.


* **String Pseudonymization** (*AES-256-CTR* ):
  * Implements **AES-256-CTR** for variable-sized strings (`VARSIZED`).
  * Generates a deterministic Synthetic Initialization Vector (SIV) via HMAC-SHA256 over the plaintext to avoid storing state across continuous stream windows.
  * Prepends the hex-encoded IV to the ciphertext output.

---

### ⚙️ Environment Configuration

Following the Twelve-Factor App methodology, cryptographic secrets are strictly decoupled from the source code and managed via runtime environment variables.

You can either export the key directly in your terminal session or define it within a `.env` file located in the root directory of your NebulaStream workspace.

* **Option A: Direct Terminal Export**
  ```bash
  export PSEUDONYM_SECRET_KEY="your-secure-cryptographic-key"
  ```

* **Option B: Using a Root .env File**:
Create a file named .env in the root directory of the project with the following content:
    ```bash
    PSEUDONYM_SECRET_KEY=your-secure-cryptographic-key  
    ```

**Fail-Safe Design**: If the PSEUDONYM_SECRET_KEY environment variable is missing upon operator initialization, the system throws a std::runtime_error during the query deployment phase, failing closed to prevent any accidental plaintext leaks.

---

### 🚀 Usage in NebulaStream Queries

Once compiled and registered via NebulaStream's plugin architecture, the operators can be invoked natively within stream processing queries:

**Pseudonymization:**
```SQL
SELECT Pseudonymize(patient_id), Pseudonymize(patient_name) FROM medical_stream;
```

**Depseudonymization:**
```SQL
SELECT Depseudonymize(pseudo_id) FROM secured_stream;
```