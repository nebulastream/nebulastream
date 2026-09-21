# BLAS Codon compatibility plugin

This native-only plugin exposes the small BLAS ABI subset admitted for inline Codon UDFs. It links OpenBLAS and places the address of OpenBLAS's actual `cblas_sdot` implementation in its plugin descriptor without exporting the function process-wide.
