{ pkgs }:
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
        libcxx = "25a231fc592414b80a6d5ecd7742419ebac83cbae74db7150f6e87beb296a75d";
        libstdcxx = "d29dc27ad7294dce0f4d198e3e1575bd6d2edfa159549472a2e9570b4e68a6e7";
      };
      address = {
        libcxx = "3813e490dc0618f8dad357bca4209c1556409194e7a963b70ffea463de0c62ee";
        libstdcxx = "8cc6ee795a75c813e7f729af1831b78e72478d091ca0325a2fc74c641f836b96";
      };
      thread = {
        libcxx = "40d691e9723b0b97d1b3a164e2470f31e102768547cbbd227791130ae3befb1e";
        libstdcxx = "43d3d9e66c1019257a91d05de8ffb833ba27f07ef9183c8d039fbeb75611964b";
      };
      undefined = {
        libcxx = "2f6fbb70bb8833b550ad49cdbb3b9d5281a23aed44c854b99685dff65cac65fc";
        libstdcxx = "cc4363b35599e2af0a80403e9c6d2b8efb64310beb07d1c478a6acc067d814d6";
      };
    };
    arm64 = {
      none = {
        libcxx = "a48967dea51c6d0692b989647e29a93b255867e29c90e4d5f59905ca471e38ab";
        libstdcxx = "a8dddaa87cd65f64e51def3974dca8dfd996c634e607d694d20a2bc724dd57d9";
      };
      address = {
        libcxx = "6ba3b6b783f144e64c4d80296ed8ed6a978eb6aff7d8560d0cfd0160ed55bf77";
        libstdcxx = "f519bf22b7a02a93a80df74ba78d9e970eb639d809cee99b38c7e42d763aea0b";
      };
      thread = {
        libcxx = "70b5b8bb514840f51a5c3513f6ce75d68dfb293e2778156e937a12fa394d364b";
        libstdcxx = "50a04267402defb66b44c67e6f599b7110d66939d9b0f0d59c50f3e5ffb731e9";
      };
      undefined = {
        libcxx = "40312cab075ded317870fdaf2d721ca681ed898d57403c340342531b2cee8751";
        libstdcxx = "ba11312f3b793358d6c72c657061c4560cd281f5b658e0b5b923b9a3b1c04ed9";
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
      url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-22-clang19/nes-llvm-22-clang19-${arch}-${sanitizer}-${stdlib}.tar.zstd";
    in
    stdenv.mkDerivation {
      pname = "nes-mlir";
      version = "22";

      src = pkgs.fetchurl {
        inherit url;
        sha256 = hash;
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
