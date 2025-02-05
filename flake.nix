{
  description = "Dev env for shared data processor";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      perSystem = { config, self', inputs', pkgs, system, ... }: {
        formatter = pkgs.alejandra;
        devShells.default = pkgs.mkShell {
          name = "dev shell";
          packages = with pkgs; [
            python312
            ruff-lsp
            uv
            yaml-language-server

            scala-cli
            metals
          ];
        };
      };
    };
}
