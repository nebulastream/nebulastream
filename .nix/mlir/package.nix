{
  pkgs,
  llvmToolchainVersion,
  mlirRelease,
  mlirAssetPrefix,
}:
let
  lib = pkgs.lib;
  stdenv = pkgs.stdenvNoCC;
  platform = pkgs.stdenv.hostPlatform;
  arch =
    if platform.isAarch64 then
      "arm64"
    else if platform.isx86_64 then
      "x64"
    else
      builtins.throw "Unsupported system: ${platform.system}";

  supportedSanitizers = [
    "none"
    "address"
    "thread"
    "undefined"
  ];
  supportedStdlibs = [
    "libcxx"
    "libstdcxx"
  ];

  hashes = {
    x64 = {
      none = {
        libcxx = "sha256-vVLdgd+8Fw7OfyJM5bfMO0Gv/bxI19rg9KK8VhNSfAE=";
        libstdcxx = "sha256-8HTBvceq7FSX2OGJiFVxk1s6tjhu3Zf9f7eddakfJEA=";
      };
      address = {
        libcxx = "sha256-82xPmfsxNUfa8bNyMT3EqpvrCnOge/qfdYeXVMNwH2g=";
        libstdcxx = "sha256-Q1HLol2DW4yJhIkxVGXHPPBnkSKvkAc/ft0ju+dvTaQ=";
      };
      thread = {
        libcxx = "sha256-tdACHM97MABTUX8LCfyhWWjBkvXhGAwl3yspil/I47o=";
        libstdcxx = "sha256-qZYBL6Y8bf9ENii0npEuWS6rEx6GPwjWtJOkps+d4HU=";
      };
      undefined = {
        libcxx = "sha256-zjMYm+zDryNfJjgs8uG9CwE299GaBvEB6g7nVJSyzvw=";
        libstdcxx = "sha256-S9XdjkMC/S9yJetoU3Sbi13Ap/W91lS4AwChZcgxNuo=";
      };
    };
    arm64 = {
      none = {
        libcxx = "sha256-BskqEdrNOmPWNV7PD8DzZWxiZJykWEyUUsQIyQcLwqI=";
        libstdcxx = "sha256-8IqoEgNximKB49/Gew14Ai/LGKadsNw1bBB01BGaqcM=";
      };
      address = {
        libcxx = "sha256-JaSaWZJ7r5mcPZFvQruCL+KEdkbpa5fC0b/PdYE9R04=";
        libstdcxx = "sha256-6NC4auWCVv59oQPST7LZHWsSWqM7F8MLnRJdnAGah7o=";
      };
      thread = {
        libcxx = "sha256-2vsHxTXiFRmukdlxvaATtVNle5zVyel+G/bnPD5LSW8=";
        libstdcxx = "sha256-H9XrQekxGGu60hHiEGdMMr+rjR9bYe42gkaogsgDbvw=";
      };
      undefined = {
        libcxx = "sha256-T8Ft/9ASprmoByPksquNMN3AJe+euorKPzEiPUypYpc=";
        libstdcxx = "sha256-0maAVCipzhJvFuzUzwE/U0at+vmW0dVEzQekOskLM1Y=";
      };
    };
  };

  sanitizeSelection =
    list: value:
    if lib.elem value list then value else builtins.throw "Unsupported selection '${value}'";

  hashFor =
    { sanitizer, stdlib }:
    let
      san = sanitizeSelection supportedSanitizers sanitizer;
      std = sanitizeSelection supportedStdlibs stdlib;
      archHashes = hashes.${arch};
    in
    archHashes.${san}.${std};

  mkMlirBinary =
    { sanitizer, stdlib }:
    let
      hash = hashFor { inherit sanitizer stdlib; };
      url = "https://github.com/nebulastream/clang-binaries/releases/download/${mlirRelease}/${mlirAssetPrefix}-${arch}-${sanitizer}-${stdlib}.tar.zstd";
    in
    stdenv.mkDerivation {
      pname = "nes-mlir";
      version = llvmToolchainVersion;

      src = pkgs.fetchurl {
        inherit url hash;
      };

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

  normalizeArgs =
    args:
    let
      sanitizer = args.sanitizer or "none";
      stdlib = args.stdlib or "libstdcxx";
    in
    {
      inherit sanitizer stdlib;
    };

in
{
  forOptions =
    args:
    let
      cfg = normalizeArgs args;
    in
    mkMlirBinary cfg;

  mlirBinary = mkMlirBinary {
    sanitizer = "none";
    stdlib = "libstdcxx";
  };
}
