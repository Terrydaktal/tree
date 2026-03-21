# tree

A modern, high-performance version of the `tree` utility written in Rust.

## Features

- **Blazing Fast**: Uses `jwalk` for parallel directory traversal.
- **Modern Terminal Support**: Includes **OSC 8 Hyperlinks** (clickable files and directories) and respects the `LS_COLORS` environment variable.
- **Smart Truncation**: Always displays everything at the top level (depth 1), but truncates subdirectories (depth 2+) based on the `-T <value>` parameter with an `... and x more` entry.
- **Type Classification**: Supports the `-F` flag to add suffixes (`/` for directories, `@` for symbolic links, and `*` for executables).
- **Optimized**: Built with `jemalloc` for memory efficiency and compiled with `target-cpu=native` for maximum performance on your hardware.

## Installation

### Prerequisites

- [Rust](https://www.rust-lang.org/tools/install) (latest stable or 2024 edition support)

### Building from Source

```bash
git clone https://github.com/Terrydaktal/tree.git
cd tree
cargo build --release
```

The optimized binary will be available at `./target/release/tree`.

### System Integration (Linux/macOS)

```bash
ln -sf $(pwd)/target/release/tree ~/.local/bin/tree
```

## Usage

```bash
tree [OPTIONS] [PATH]
```

### Options

- `-a`: Show hidden files.
- `-L <MAX_DEPTH>`: Max depth to display (default: 100).
- `-F`: Classify (add `/` for dirs, `@` for symlinks, `*` for executables).
- `-T, --trunc <TRUNC>`: Truncate depth 2+ entries to this value (default: 10).
- `--sizes`: Show file sizes.
- `--times`: Show file modification times.
- `--hyperlinks`: Enable OSC 8 hyperlinks (off by default).
- `-h, --help`: Print help.
- `-V, --version`: Print version.

## Project Structure

- `src/main.rs`: The core logic for directory traversal, tree construction, and rendering.
- `Cargo.toml`: Dependency and optimization profile configuration.
- `.cargo/config.toml`: Architecture-specific optimization flags.

## License

MIT
