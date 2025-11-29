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
        libcxx = "0fw9ivnahhcdjz12dbi7zdmxkxkc16f1injai90a99kvi5jknkcc";
        libstdcxx = "0ymf7x74arflvxjcyy9manfxwzvdlcanms4n4wwk118ssrdqvydf";
      };
      address = {
        libcxx = "1mm6hc3f8z5l6d78id5vc9k76fb5s1pqqpxxqdvrzdd63h44yw04";
        libstdcxx = "1zv8ljrbkv914afcmw2ybqbsmskyd7nmaf6v19qg94hyimjnikv5";
      };
      thread = {
        libcxx = "18d34bv9f0zvha65i4406ggbdk8plb1pmqhdj6kbdzr85j3xfd44";
        libstdcxx = "1mvyqmx1jybggkljb1gvr726i2c0flw6wcbgb56bzxfnzc52hnsh";
      };
      undefined = {
        libcxx = "004ny9dik6ngi9plwjjzd36q7grb2rrdxircwgv3d48pd7jww85m";
        libstdcxx = "0mfl87218ci1cqfrsp41hmnq2zfiv3p5cxvrw4mr9v2yh50vlgn8";
      };
    };
    arm64 = {
      none = {
        libcxx = "02l6k7lxhh78884xbq1gp2ms1qcy1v3ycz4pcp1wzhikyxllfrpf";
        libstdcxx = "0pk83vhf3rd5s1pjv6vrmywk11gqvk5b42dyd9jarvv5x0jc0x1y";
      };
      address = {
        libcxx = "0yrj1967zq081m2wfrfdmqf35qpfma704dvjrw6p551dr0ffsqv0";
        libstdcxx = "08q5hz1bayy969lmrvx8kvl33bywlhxr6i9jjvw1q8gr2wspfqqg";
      };
      thread = {
        libcxx = "1kssjk6mc4sw80s8ad8havyb07hga40vd7wrjr27spnmxxida104";
        libstdcxx = "1k1mb7pvsigjdkwnvxm6a91ii8kvbwchksaw2hig9arpw13dsqbh";
      };
      undefined = {
        libcxx = "06fbjswigang29pmbp2w6dvrin8gppjfcfks5rx53p5vxh5mm360";
        libstdcxx = "1p945dg9prmzskb7zgcpdphh538r3d4fd76i5wg26s32p9jqxixr";
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
      url = "https://github.com/nebulastream/clang-binaries/releases/download/vmlir-20/nes-llvm-20-${arch}-${sanitizer}-${stdlib}.tar.zstd";
    in
    stdenv.mkDerivation {
      pname = "nes-mlir";
      version = "20";

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
