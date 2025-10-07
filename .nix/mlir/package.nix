{ pkgs }:
let
  lib = pkgs.lib;
  stdenv = pkgs.stdenvNoCC;
  platform = pkgs.stdenv.hostPlatform;
  srcInfo =
    if platform.isAarch64 then {
      url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-arm64-none-libstdcxx.tar.zstd";
      sha256 = "3e74c024e865efac646abe09b2cadcf88530b9af799b2d6fd0a5e5e1e01e685e";
    } else if platform.isx86_64 then {
      url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-x64-none-libstdcxx.tar.zstd";
      sha256 = "aef98d5bd61a8530392796e86a15a36d7fde9d553579cf64dfd465454e3fae7a";
    } else
      builtins.throw "Unsupported system: ${platform.system}";
in {
  mlirBinary = stdenv.mkDerivation {
    pname = "nes-mlir";
    version = "20";

    src = pkgs.fetchurl srcInfo;

    nativeBuildInputs = [
      pkgs.zstd
      pkgs.gnutar
    ];

    dontUnpack = true;
    strictDeps = true;

    installPhase = ''
      runHook preInstall
      mkdir -p "$TMPDIR/mlir"
      zstd -d "$src" --stdout | tar -xf - -C "$TMPDIR/mlir"
      mkdir -p "$out"
      cp -r "$TMPDIR/mlir"/clang/* "$out"/
      runHook postInstall
    '';

    meta = with lib; {
      description = "Prebuilt MLIR toolchain used by NebulaStream";
      license = licenses.asl20;
      platforms = platforms.linux;
    };
  };
}
