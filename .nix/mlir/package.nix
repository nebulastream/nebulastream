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
        libcxx = "0a252bf34873dd068d98ca0ee30866f949ac855142502fb7d79e77494e82ee4c";
        libstdcxx = "5a3a1b41e847aad5ff7bdbb3cbdf9a156200a0fc8fb3e77144edf2af871f4481";
      };
      address = {
        libcxx = "1c8d988505211703203ed707942eb1ea0967b6f56f6f50e6ff7cbc179d4c5f18";
        libstdcxx = "5e8ad701a13f79a88c7a1700be812a4627e4532c03ed6408bffab67bb3294dc1";
      };
      thread = {
        libcxx = "46ebfd5e0de062442fc315c7ca3c08c089b3fe83515ecc5c1f8a88166b88d2d5";
        libstdcxx = "1ca03576341cd0be6664bb709690c4a3eb395deddc2aca2ed5d17e21594d23fc";
      };
      undefined = {
        libcxx = "b295d56ba321c8af7685ebdc0a46824893e2aa426ae8b2e99df8668992fcaef7";
        libstdcxx = "90a5ee715003b81ee21ee4e540da4201c451bd099b3f668616976cfa84fe67fd";
      };
    };
    arm64 = {
      none = {
        libcxx = "4ad7b0a52396f422f0ab1a0c443a573bf2038f7a062435d01f64073af21d4f04";
        libstdcxx = "132ad62eeb7842c030419dfc2cdd01b3c1ecfaeca98c06e0096fdb27504a7805";
      };
      address = {
        libcxx = "35989e505e8210306d18dfc2386ba44b68873ca616ddc2f6136e969b98c1bcb2";
        libstdcxx = "115ae6bfc52db379a2b812bb834924864e19d5a6f8ac6eb77469e6141c69296c";
      };
      thread = {
        libcxx = "2367380acd49dcb816b442b0129adf89112f2479bf37b5be9d322ca9be11c73f";
        libstdcxx = "2f42a72b7fc049cd44fd61208f44b1f4e18df703cf98dfd8bede465beb4a6214";
      };
      undefined = {
        libcxx = "a602dab26489d6ce70aeb6335e09669fb17e7b1d4a73b098607b22fbbcb95dd1";
        libstdcxx = "4ea40f9f06f24572276fe8ff7862e11500f0c1ddff06197a66e8c02d80169bd2";
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
      url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-21-v7/nes-llvm-21-v3-${arch}-${sanitizer}-${stdlib}.tar.zstd";
    in
    stdenv.mkDerivation {
      pname = "nes-mlir";
      version = "21";

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
