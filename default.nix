{ callPackage,stdenv, overrideCC,
    cmake, pkg-config, clang-tools, papi, lib, jemalloc
    }:

stdenv.mkDerivation {
    name = "NES";
    src = if lib.inNixShell then null else ./.;

    nativeBuildInputs = [ cmake pkg-config clang-tools ];
    buildInputs = [
        papi jemalloc
    ];
}








