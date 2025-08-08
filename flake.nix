{
  description = "NebulaStream development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs { inherit system; };
        llvm = pkgs.llvmPackages_19;

        # MLIR 20 binary release
        mlirBinary = pkgs.stdenv.mkDerivation rec {
          pname = "nes-mlir";
          version = "20";

          src =
            if system == "aarch64-linux" then
              pkgs.fetchurl {
                url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-arm64-none-libcxx.tar.zstd";
                sha256 = "ee664769f733c2cfc365977ce6c70e9ee1a0abb82fe0d50942e840d8e999860a";
              }
            else if system == "x86_64-linux" then
              pkgs.fetchurl {
                url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-x64-none-libcxx.tar.zstd";
                sha256 = "8c4d3b65897ba6a4408a4ada189c096cf6d96bfb27ae26c2978d41a8ec8e893b";
              }
            else
              throw "Unsupported system: ${system}";

          nativeBuildInputs = [ pkgs.zstd ];

          unpackPhase = ''
            runHook preUnpack
            mkdir -p $TMP/mlir
            cd $TMP/mlir
            zstd -d $src --stdout | tar -xf -
            runHook postUnpack
          '';

          installPhase = ''
            runHook preInstall
            mkdir -p $out
            cp -r clang/* $out/
            runHook postInstall
          '';

          dontFixup = true;
        };

        # Core development tools
        buildTools = with pkgs; [
          cmake
          ninja
          pkg-config
          git
          ccache
        ];

        # LLVM 19 toolchain with versioned symlinks for vcpkg
        clangWithVersions = pkgs.symlinkJoin {
          name = "clang-with-versions";
          paths = [ llvm.clang ];
          postBuild = ''
            ln -s $out/bin/clang $out/bin/clang-19
            ln -s $out/bin/clang++ $out/bin/clang++-19
          '';
        };

        # LLVM 19 toolchain
        llvmTools = [
          llvm.llvm
          clangWithVersions
          llvm.lld
          llvm.libcxx
          llvm.clang-tools # clang-format, clang-tidy
        ];

        # Runtime dependencies for MLIR
        mlirDeps = with pkgs; [
          libffi
          libxml2
          openjdk21
        ];

        # Development tools
        devTools = with pkgs; [
          gdb
          llvm.lldb
          python3
        ];
      in
      {
        formatter = pkgs.nixfmt-tree;

        devShells.default = pkgs.mkShell {
          packages = buildTools ++ llvmTools ++ mlirDeps ++ devTools;

          shellHook = ''
            export LLVM_TOOLCHAIN_VERSION=19

            # Configure LLVM toolchain
            export CC=${llvm.clang}/bin/clang
            export CXX=${llvm.clang}/bin/clang++
            export LD=${llvm.lld}/bin/lld
            export AR=${llvm.llvm}/bin/llvm-ar
            export STRIP=${llvm.llvm}/bin/llvm-strip
            export RANLIB=${llvm.llvm}/bin/llvm-ranlib
            export OBJCOPY=${llvm.llvm}/bin/llvm-objcopy
            export OBJDUMP=${llvm.llvm}/bin/llvm-objdump
            export READELF=${llvm.llvm}/bin/llvm-readelf

            # Configure libc++
            export CXXFLAGS="-nostdinc++ -isystem ${llvm.libcxx.dev}/include/c++/v1"
            export LDFLAGS="-stdlib=libc++ -L${llvm.libcxx}/lib"
            export CMAKE_CXX_FLAGS="-nostdinc++ -isystem ${llvm.libcxx.dev}/include/c++/v1"
            export CMAKE_EXE_LINKER_FLAGS="-stdlib=libc++"
            export CMAKE_REQUIRED_FLAGS="-nostdinc++"
            export CMAKE_REQUIRED_INCLUDES="${llvm.libcxx.dev}/include/c++/v1"
            unset CPLUS_INCLUDE_PATH

            # Configure build environment with MLIR
            export MLIR_DIR=${mlirBinary}/lib/cmake/mlir
            export LLVM_DIR=${mlirBinary}/lib/cmake/llvm
            export CMAKE_PREFIX_PATH="${mlirBinary}:''${CMAKE_PREFIX_PATH:-}"
            export CMAKE_GENERATOR=Ninja
            export VCPKG_ENV_PASSTHROUGH="MLIR_DIR;LLVM_DIR;CMAKE_PREFIX_PATH"
            export IN_NIX_SHELL=1
            unset NES_PREBUILT_VCPKG_ROOT

            # Configure library paths
            export PKG_CONFIG_PATH="${pkgs.libffi}/lib/pkgconfig:${pkgs.libxml2.dev}/lib/pkgconfig:''${PKG_CONFIG_PATH:-}"
            export FFI_INCLUDE_DIR="${pkgs.libffi.dev}/include"
            export FFI_LIBRARY_DIR="${pkgs.libffi}/lib"
            export LD_LIBRARY_PATH="${llvm.libcxx}/lib:${pkgs.stdenv.cc.cc.lib}/lib:${pkgs.glibc}/lib:${pkgs.libffi}/lib:${pkgs.libxml2}/lib:''${LD_LIBRARY_PATH:-}"
          '';
        };
      }
    );
}
