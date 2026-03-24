use chrono::{DateTime, Local};
use clap::Parser;
use dashmap::{DashMap, DashSet};
use jwalk::WalkDir;
use lscolors::LsColors;
use rustc_hash::FxBuildHasher;
use std::ffi::OsString;
use std::collections::{HashMap, HashSet};
use std::cmp::Ordering;
use std::fs::Metadata;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use url::Url;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about = "A modern tree clone in Rust using jwalk")]
struct Args {
    /// Directory to list
    #[arg(value_name = "PATH")]
    path: Option<PathBuf>,

    /// Toggle showing hidden files (double application cancels: -aa)
    #[arg(short = 'a', action = clap::ArgAction::Count)]
    all_toggles: u8,

    /// Max depth to display
    #[arg(short = 'L', default_value = "100", overrides_with = "max_depth")]
    max_depth: usize,

    /// Classify (add / for dirs, * for executables)
    #[arg(short = 'F', overrides_with = "classify")]
    classify: bool,

    /// Truncate depth 2+ entries to this value
    #[arg(short = 'T', long, default_value = "10", overrides_with = "trunc")]
    trunc: usize,

    /// Hide the "... and N more" summary rows
    #[arg(short = 'M', long = "hide-more-count", overrides_with = "hide_more_count")]
    hide_more_count: bool,

    /// Alias for -L 20 -T 2
    #[arg(long = "deep", overrides_with = "deep")]
    deep: bool,

    /// Show directories only
    #[arg(short = 'd', long = "dirs-only", overrides_with = "dirs_only")]
    dirs_only: bool,

    /// Toggle .git expansion behavior (double application cancels: -GG)
    #[arg(short = 'G', long = "no-expand-git", action = clap::ArgAction::Count)]
    no_expand_git_toggles: u8,

    /// Enable OSC 8 hyperlinks
    #[arg(long, overrides_with = "hyperlinks")]
    hyperlinks: bool,

    /// Follow symbolic links
    #[arg(short = 'H', long, overrides_with = "follow_links")]
    follow_links: bool,

    /// Show proper recursive directory sizes
    #[arg(short = 's', long, overrides_with = "sizes")]
    sizes: bool,

    /// Show file modification times
    #[arg(short = 't', long, overrides_with = "times")]
    times: bool,

    /// Show total recursive directory and file counts
    #[arg(short = 'c', long, overrides_with = "counts")]
    counts: bool,

    /// Alias for -stc (show sizes, times, and counts)
    #[arg(short = 'l', overrides_with = "long_listing")]
    long_listing: bool,

    /// Reverse the final displayed output lines
    #[arg(short = 'r', long, overrides_with = "reverse")]
    reverse: bool,

    /// Cache shown output paths to ~/.cache/universal-last-dirs and ~/.cache/universal-last-files
    #[arg(long, overrides_with = "cache_raw")]
    cache_raw: bool,

    /// Sort all levels by field and order (e.g. -S size desc)
    #[arg(short = 'S', long, num_args = 2, value_names = ["FIELD", "ORDER"], overrides_with = "sort")]
    sort: Option<Vec<String>>,

    /// Number of threads to use
    #[arg(short = 'j', long, default_value = "8", overrides_with = "threads")]
    threads: usize,
}

fn format_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    const TIB: u64 = GIB * 1024;

    if bytes >= TIB {
        format!("{:.1} TiB", bytes as f64 / TIB as f64)
    } else if bytes >= GIB {
        format!("{:.1} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} B", bytes)
    }
}

fn format_time(metadata: &Metadata) -> String {
    if let Ok(mtime) = metadata.modified() {
        let datetime: DateTime<Local> = mtime.into();
        datetime.format("%Y-%m-%d %H:%M").to_string()
    } else {
        "-".to_string()
    }
}

fn format_count(value: u64) -> String {
    let s = value.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn count_num_len(value: u64) -> usize {
    format_count(value).len()
}

#[derive(Clone, Copy)]
struct CountColumnLayout {
    dir_width: usize,
    file_width: usize,
}

impl CountColumnLayout {
    fn pair_width(self) -> usize {
        self.dir_width + 1 + 1 + self.file_width + 1 // "<dir>d <file>f"
    }
}

fn print_recursive_count_pair(
    dir_count: u64,
    file_count: u64,
    count_layout: CountColumnLayout,
) {
    let dir_str = format_count(dir_count);
    let file_str = format_count(file_count);
    let count_color = "\x1b[1;33m";
    let color_reset = "\x1b[0m";
    print!("{}{}{}d", count_color, dir_str, color_reset);
    if count_layout.dir_width > dir_str.len() {
        print!("{:width$}", "", width = count_layout.dir_width - dir_str.len());
    }
    print!(" ");
    print!("{}{}{}f", count_color, file_str, color_reset);
    if count_layout.file_width > file_str.len() {
        print!("{:width$}", "", width = count_layout.file_width - file_str.len());
    }
    print!(" ");
}

fn compute_count_column_layout(node: &Node, args: &Args) -> CountColumnLayout {
    let mut dir_width = 1usize;
    let mut file_width = 1usize;

    for child in &node.children {
        dir_width = dir_width.max(count_num_len(child.recursive_dir_count));
        file_width = file_width.max(count_num_len(child.recursive_file_count));
        if child.is_dir {
            let nested = compute_count_column_layout(child, args);
            dir_width = dir_width.max(nested.dir_width);
            file_width = file_width.max(nested.file_width);
        }
    }

    let child_count = node.children.len();
    if node.total_children_count > child_count
        && !args.hide_more_count
        && (!args.dirs_only || node.omitted_dirs_count > 0)
    {
        dir_width = dir_width.max(count_num_len(node.omitted_recursive_dir_count));
        file_width = file_width.max(count_num_len(node.omitted_recursive_file_count));
    }

    CountColumnLayout {
        dir_width,
        file_width,
    }
}

fn write_path_list(cache_path: &Path, paths: &[PathBuf]) -> std::io::Result<()> {
    let mut output = String::new();
    for path in paths {
        output.push_str(&path.to_string_lossy());
        output.push('\n');
    }
    std::fs::write(cache_path, output)
}

fn write_cache_raw_paths(dir_paths: &[PathBuf], file_paths: &[PathBuf]) -> std::io::Result<()> {
    let cache_dir = if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home).join(".cache")
    } else {
        PathBuf::from(".cache")
    };

    std::fs::create_dir_all(&cache_dir)?;
    write_path_list(&cache_dir.join("universal-last-dirs"), dir_paths)?;
    write_path_list(&cache_dir.join("universal-last-files"), file_paths)
}

fn to_full_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .unwrap_or_else(|_| path.to_path_buf())
    }
}

fn is_executable_path(path: &Path) -> bool {
    std::fs::symlink_metadata(path)
        .map(|md| md.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

fn cmp_name(a: &str, b: &str) -> Ordering {
    a.to_ascii_lowercase()
        .cmp(&b.to_ascii_lowercase())
        .then_with(|| a.cmp(b))
}

struct Node {
    path: PathBuf,
    name: String,
    metadata: Option<Metadata>,
    children: Vec<Node>,
    total_children_count: usize,
    omitted_size: u64,
    omitted_recursive_dir_count: u64,
    omitted_recursive_file_count: u64,
    omitted_dirs_count: usize,
    omitted_files_count: usize,
    is_dir: bool,
    is_symlink: bool,
    true_size: u64,
    recursive_dir_count: u64,
    recursive_file_count: u64,
}

#[derive(Clone)]
struct EntryStub {
    name: String,
    path: PathBuf,
    metadata: Option<Metadata>,
    file_type: std::fs::FileType,
    is_symlink: bool,
}

struct ScanResult {
    dir_children: HashMap<PathBuf, Vec<EntryStub>, FxBuildHasher>,
    true_sizes: HashMap<PathBuf, u64, FxBuildHasher>,
    true_dir_counts: HashMap<PathBuf, u64, FxBuildHasher>,
    true_file_counts: HashMap<PathBuf, u64, FxBuildHasher>,
}

fn sort_requires_metadata(sort: &Option<Vec<String>>) -> bool {
    sort.as_ref()
        .and_then(|v| v.first())
        .map(|field| matches!(field.to_ascii_lowercase().as_str(), "size" | "time" | "date" | "mtime"))
        .unwrap_or(false)
}

fn metadata_required(args: &Args) -> bool {
    args.sizes
        || args.times
        || args.follow_links
        || sort_requires_metadata(&args.sort)
}

fn main() {
    let mut args = Args::parse();
    const REVERSE_ENV_KEY: &str = "TREE_INTERNAL_REVERSE";

    if args.long_listing {
        args.sizes = true;
        args.times = true;
        args.counts = true;
    }

    let show_all = args.all_toggles % 2 == 1;

    // .git expansion precedence:
    // - default: do not expand
    // - `-a` expands
    // - each `-G` flips the state (`-G -G` cancels back)
    let mut no_expand_git = !show_all;
    if args.no_expand_git_toggles % 2 == 1 {
        no_expand_git = !no_expand_git;
    }

    if args.deep {
        args.max_depth = 20;
        args.trunc = 2;
    }

    if args.reverse && std::env::var_os(REVERSE_ENV_KEY).is_none() {
        let exe = std::env::current_exe().expect("failed to resolve current executable");
        let forwarded_args: Vec<OsString> = std::env::args_os()
            .skip(1)
            .filter_map(|arg| {
                let text = arg.to_string_lossy();
                if text == "-r" || text == "--reverse" {
                    return None;
                }
                if text.starts_with('-') && !text.starts_with("--") && text.len() > 2 && text.contains('r') {
                    let kept: String = text[1..].chars().filter(|&c| c != 'r').collect();
                    if kept.is_empty() {
                        None
                    } else {
                        Some(OsString::from(format!("-{}", kept)))
                    }
                } else {
                    Some(arg)
                }
            })
            .collect();

        let output = Command::new(exe)
            .args(&forwarded_args)
            .env(REVERSE_ENV_KEY, "1")
            .output()
            .expect("failed to execute reverse output pass");

        if !output.status.success() && !output.stderr.is_empty() {
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let lines: Vec<String> = stdout.lines().map(|s| s.to_string()).collect();
        let strip_ansi = |line: &str| -> String {
            let bytes = line.as_bytes();
            let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
            let mut i = 0usize;
            while i < bytes.len() {
                if bytes[i] == 0x1b && i + 1 < bytes.len() && bytes[i + 1] == b'[' {
                    i += 2;
                    while i < bytes.len() {
                        let b = bytes[i];
                        i += 1;
                        if b.is_ascii_alphabetic() {
                            break;
                        }
                    }
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            String::from_utf8_lossy(&out).into_owned()
        };
        let plain_lines: Vec<String> = lines.iter().map(|s| strip_ansi(s)).collect();
        let mut reversed: Vec<String> = lines.into_iter().rev().collect();
        let reversed_plain: Vec<String> = plain_lines.into_iter().rev().collect();
        let connector_index = |line: &str| -> Option<usize> {
            line.find("├── ")
                .or_else(|| line.find("└── "))
                .or_else(|| line.find("┌── "))
        };
        let prefix_base = reversed_plain
            .iter()
            .filter_map(|line| connector_index(line))
            .min()
            .unwrap_or(0);

        let parse_depth = |line: &str| -> Option<usize> {
            let conn_idx = connector_index(line)?;
            if conn_idx < prefix_base {
                return None;
            }
            let tree_prefix = &line[prefix_base..conn_idx];
            let mut depth = 0usize;
            let mut rest = tree_prefix;
            while !rest.is_empty() {
                if let Some(next) = rest.strip_prefix("│   ") {
                    depth += 1;
                    rest = next;
                    continue;
                }
                if let Some(next) = rest.strip_prefix("    ") {
                    depth += 1;
                    rest = next;
                    continue;
                }
                return None;
            }
            Some(depth)
        };

        let set_conn = |line: &mut String, conn: &str| {
            if line.contains("├── ") {
                *line = line.replacen("├── ", conn, 1);
            } else if line.contains("└── ") {
                *line = line.replacen("└── ", conn, 1);
            } else if line.contains("┌── ") {
                *line = line.replacen("┌── ", conn, 1);
            }
        };

        let depths: Vec<Option<usize>> = reversed_plain.iter().map(|line| parse_depth(line)).collect();
        // Reverse style: child groups use top+middle connectors only.
        // Flip every terminating connector to a top connector first.
        for line in &mut reversed {
            if line.contains("└── ") {
                set_conn(line, "┌── ");
            }
        }

        // In reverse mode, the first displayed top-level row is the top connector.
        // Remaining top-level rows are continuing rows.
        let root_indices: Vec<usize> = depths
            .iter()
            .enumerate()
            .filter_map(|(idx, depth)| if *depth == Some(0) { Some(idx) } else { None })
            .collect();
        for (pos, &idx) in root_indices.iter().enumerate() {
            if pos == 0 {
                set_conn(&mut reversed[idx], "┌── ");
            } else {
                set_conn(&mut reversed[idx], "├── ");
            }
        }

        // Replace reversed root marker '.' with an interpunct.
        if let Some(last) = reversed.last_mut() {
            if strip_ansi(last).trim() == "." {
                *last = if prefix_base > 0 {
                    format!("{:width$}·", "", width = prefix_base)
                } else {
                    "·".to_string()
                };
            }
        }

        for line in reversed {
            println!("{}", line);
        }

        std::process::exit(output.status.code().unwrap_or(1));
    }

    // Configure Rayon thread pool
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads)
        .build_global();

    let lscolors = LsColors::from_env().unwrap_or_default();
    let use_hyperlinks = args.hyperlinks;

    let root_path = args.path.as_ref().map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    let root_abs = root_path.canonicalize().unwrap_or_else(|_| root_path.clone());
    let need_metadata = metadata_required(&args);
    
    // PHASE 1: Single Unified Parallel Scan
    // `-L` caps scan depth unless recursive aggregates are requested.
    let mut scan_max_depth = if args.sizes || args.counts {
        usize::MAX
    } else {
        args.max_depth
    };
    if no_expand_git {
        if root_abs
            .file_name()
            .map(|name| name == ".git")
            .unwrap_or(false)
        {
            scan_max_depth = 0;
        }
    }
    let scan = perform_unified_scan(
        &root_abs,
        &args,
        scan_max_depth,
        need_metadata,
        show_all,
        no_expand_git,
    );

    let root_metadata = if need_metadata {
        if args.follow_links {
            root_abs.metadata().ok()
        } else {
            root_abs.symlink_metadata().ok()
        }
    } else {
        None
    };
    let root_file_type = root_abs.symlink_metadata().ok().map(|m| m.file_type());
    
    // PHASE 2: In-Memory Tree Build (Zero Disk IO)
    let root_node = build_tree_from_cache(
        &root_abs,
        root_metadata,
        root_file_type,
        None,
        0,
        &args,
        &scan,
        no_expand_git,
    );

    let count_layout = if args.counts {
        root_node
            .as_ref()
            .map(|n| compute_count_column_layout(n, &args))
            .unwrap_or(CountColumnLayout {
                dir_width: 1,
                file_width: 1,
            })
    } else {
        CountColumnLayout {
            dir_width: 0,
            file_width: 0,
        }
    };

    let root_label = root_path.display().to_string();
    if args.counts || args.sizes || args.times {
        let prefix_base = (if args.counts { count_layout.pair_width() + 1 } else { 0 })
            + (if args.sizes { 11 } else { 0 })
            + (if args.times { 17 } else { 0 });
        println!("{:width$}{}", "", root_label, width = prefix_base);
    } else {
        println!("{}", root_label);
    }

    let mut shown_dir_paths: Vec<PathBuf> = Vec::new();
    let mut shown_file_paths: Vec<PathBuf> = Vec::new();
    if args.cache_raw {
        let root_full_path = to_full_path(&root_abs);
        if let Some(root_node_ref) = root_node.as_ref() {
            if root_node_ref.is_dir {
                shown_dir_paths.push(root_full_path);
            } else {
                shown_file_paths.push(root_full_path);
            }
        } else if root_file_type.map(|ft| ft.is_dir()).unwrap_or(true) {
            shown_dir_paths.push(root_full_path);
        } else {
            shown_file_paths.push(root_full_path);
        }
    }

    if let Some(root_node) = root_node {
        print_node(
            &root_node,
            0,
            &Vec::new(),
            &args,
            &lscolors,
            use_hyperlinks,
            count_layout,
            &mut shown_dir_paths,
            &mut shown_file_paths,
        );

        if args.cache_raw {
            if let Err(err) = write_cache_raw_paths(&shown_dir_paths, &shown_file_paths) {
                eprintln!("failed to write --cache-raw file: {}", err);
            }
        }
    } else if args.cache_raw {
        if let Err(err) = write_cache_raw_paths(&shown_dir_paths, &shown_file_paths) {
            eprintln!("failed to write --cache-raw file: {}", err);
        }
    }
}

fn perform_unified_scan(
    root: &Path,
    args: &Args,
    scan_max_depth: usize,
    collect_entry_metadata: bool,
    show_all: bool,
    no_expand_git: bool,
) -> ScanResult {
    let dir_children = Arc::new(DashMap::with_hasher(FxBuildHasher::default()));
    let collect_recursive_sizes = args.sizes;
    let collect_recursive_file_counts = args.counts;
    let collect_recursive_dir_counts = args.counts;
    let dir_local_sizes = if collect_recursive_sizes {
        Some(Arc::new(DashMap::with_hasher(FxBuildHasher::default())))
    } else {
        None
    };
    let dir_local_file_counts = if collect_recursive_file_counts {
        Some(Arc::new(DashMap::with_hasher(FxBuildHasher::default())))
    } else {
        None
    };
    let dir_local_dir_counts = if collect_recursive_dir_counts {
        Some(Arc::new(DashMap::with_hasher(FxBuildHasher::default())))
    } else {
        None
    };
    let seen_inodes = if collect_recursive_sizes {
        Some(Arc::new(DashSet::with_hasher(FxBuildHasher::default())))
    } else {
        None
    };
    let seen_dir_inodes = if args.follow_links && collect_entry_metadata {
        Some(Arc::new(DashSet::with_hasher(FxBuildHasher::default())))
    } else {
        None
    };

    // Seed root size and file-count accumulation.
    if collect_recursive_sizes {
        if let Ok(m) = root.symlink_metadata() {
            if let Some(ref si) = seen_inodes {
                si.insert((m.dev(), m.ino()));
            }
            if let Some(ref ds) = dir_local_sizes {
                ds.insert(root.to_path_buf(), m.blocks() * 512);
            }
        }
    }
    if let Some(ref dfc) = dir_local_file_counts {
        dfc.insert(root.to_path_buf(), 0);
    }
    if let Some(ref ddc) = dir_local_dir_counts {
        ddc.insert(root.to_path_buf(), 0);
    }

    let dc = Arc::clone(&dir_children);
    let ds = dir_local_sizes.as_ref().map(Arc::clone);
    let dfc = dir_local_file_counts.as_ref().map(Arc::clone);
    let ddc = dir_local_dir_counts.as_ref().map(Arc::clone);
    let si = seen_inodes.as_ref().map(Arc::clone);
    let sdi = seen_dir_inodes.as_ref().map(Arc::clone);
    let follow_links = args.follow_links;
    let render_max_depth = args.max_depth;
    let scan_root = root.to_path_buf();
    WalkDir::new(root)
        .skip_hidden(!show_all)
        .follow_links(args.follow_links)
        .max_depth(scan_max_depth)
        .parallelism(jwalk::Parallelism::RayonNewPool(args.threads))
        .process_read_dir(move |depth, path, _state, children| {
            let current_path = if path.is_absolute() {
                path.to_path_buf()
            } else {
                scan_root.join(path)
            };
            let mut local_sum = 0u64;
            let mut local_file_count = 0u64;
            let mut local_dir_count = 0u64;
            let should_cache_children = depth.unwrap_or(0) < render_max_depth;
            let mut stubs = if should_cache_children {
                Some(Vec::with_capacity(children.len()))
            } else {
                None
            };

            for entry_res in children.iter_mut().filter_map(|e| e.as_mut().ok()) {
                if no_expand_git
                    && entry_res.file_type.is_dir()
                    && entry_res.file_name.to_string_lossy() == ".git"
                {
                    entry_res.read_children_path = None;
                }

                let m = if collect_entry_metadata {
                    entry_res.metadata().ok()
                } else {
                    None
                };

                // Avoid re-descending into duplicate directory inodes reached via different
                // symlink paths when --follow-links is enabled.
                if follow_links && entry_res.file_type.is_dir() {
                    if let Some(ref metadata) = m {
                        if let Some(ref sdi_map) = sdi {
                            if !sdi_map.insert((metadata.dev(), metadata.ino())) {
                                entry_res.read_children_path = None;
                            }
                        }
                    }
                }

                if let Some(ref metadata) = m {
                    if let Some(ref si_map) = si {
                        if si_map.insert((metadata.dev(), metadata.ino())) {
                            local_sum += metadata.blocks() * 512;
                        }
                    }
                }
                if !entry_res.file_type.is_dir() {
                    local_file_count += 1;
                } else {
                    local_dir_count += 1;
                }
                
                if let Some(ref mut stubs_vec) = stubs {
                    stubs_vec.push(EntryStub {
                        name: entry_res.file_name.to_string_lossy().into_owned(),
                        path: current_path.join(&entry_res.file_name),
                        metadata: m,
                        file_type: entry_res.file_type,
                        is_symlink: entry_res.path_is_symlink(),
                    });
                }
            }

            if let Some(stubs_vec) = stubs {
                if !stubs_vec.is_empty() {
                    dc.insert(current_path.clone(), stubs_vec);
                }
            }
            // Track every scanned directory so upward aggregation can propagate
            // through directories that have 0 local blocks but non-zero descendants.
            if let Some(ref ds_map) = ds {
                *ds_map.entry(current_path.clone()).or_insert(0) += local_sum;
            }
            if let Some(ref dfc_map) = dfc {
                *dfc_map.entry(current_path.clone()).or_insert(0) += local_file_count;
            }
            if let Some(ref ddc_map) = ddc {
                *ddc_map.entry(current_path).or_insert(0) += local_dir_count;
            }
        })
        .into_iter()
        .for_each(|_| {});

    let mut true_sizes: HashMap<PathBuf, u64, FxBuildHasher> = if let Some(ds) = dir_local_sizes {
        match Arc::try_unwrap(ds) {
            Ok(map) => map.into_iter().collect(),
            Err(map) => map
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect(),
        }
    } else {
        HashMap::with_hasher(FxBuildHasher::default())
    };
    let mut true_file_counts: HashMap<PathBuf, u64, FxBuildHasher> =
        if let Some(dfc) = dir_local_file_counts {
            match Arc::try_unwrap(dfc) {
                Ok(map) => map.into_iter().collect(),
                Err(map) => map
                    .iter()
                    .map(|entry| (entry.key().clone(), *entry.value()))
                    .collect(),
            }
        } else {
            HashMap::with_hasher(FxBuildHasher::default())
        };
    let mut true_dir_counts: HashMap<PathBuf, u64, FxBuildHasher> = if let Some(ddc) = dir_local_dir_counts {
        match Arc::try_unwrap(ddc) {
            Ok(map) => map.into_iter().collect(),
            Err(map) => map
                .iter()
                .map(|entry| (entry.key().clone(), *entry.value()))
                .collect(),
        }
    } else {
        HashMap::with_hasher(FxBuildHasher::default())
    };

    let mut path_set: HashSet<PathBuf, FxBuildHasher> = HashSet::with_hasher(FxBuildHasher::default());
    path_set.extend(true_sizes.keys().cloned());
    path_set.extend(true_file_counts.keys().cloned());
    path_set.extend(true_dir_counts.keys().cloned());
    let mut paths: Vec<PathBuf> = path_set.into_iter().collect();
    paths.sort_unstable_by_key(|p| std::cmp::Reverse(p.components().count()));

    for path in paths {
        if path == root {
            continue;
        }
        if let Some(parent) = path.parent() {
            if !(parent.starts_with(root) || parent == root) {
                continue;
            }
            let parent_path = parent.to_path_buf();
            if let Some(value) = true_sizes.get(&path).copied() {
                *true_sizes.entry(parent_path.clone()).or_insert(0) += value;
            }
            if let Some(value) = true_file_counts.get(&path).copied() {
                *true_file_counts.entry(parent_path.clone()).or_insert(0) += value;
            }
            if let Some(value) = true_dir_counts.get(&path).copied() {
                *true_dir_counts.entry(parent_path).or_insert(0) += value;
            }
        }
    }

    let dir_children = match Arc::try_unwrap(dir_children) {
        Ok(map) => map.into_iter().collect(),
        Err(map) => map
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect(),
    };

    ScanResult {
        dir_children,
        true_sizes,
        true_dir_counts,
        true_file_counts,
    }
}

fn build_tree_from_cache(
    path: &Path,
    metadata: Option<Metadata>,
    file_type: Option<std::fs::FileType>,
    is_symlink_hint: Option<bool>,
    depth: usize,
    args: &Args,
    scan: &ScanResult,
    no_expand_git: bool,
) -> Option<Node> {
    let is_dir = metadata
        .as_ref()
        .map(|m| m.is_dir())
        .or_else(|| file_type.map(|ft| ft.is_dir()))
        .unwrap_or(false);
    let is_symlink = is_symlink_hint.unwrap_or_else(|| file_type.map(|ft| ft.is_symlink()).unwrap_or(false));

    let true_size = if args.sizes && is_dir {
        scan.true_sizes.get(path).map(|v| *v).unwrap_or(0)
    } else {
        metadata.as_ref().map(|m| m.blocks() * 512).unwrap_or(0)
    };
    let recursive_dir_count = if args.counts && is_dir {
        scan.true_dir_counts.get(path).copied().unwrap_or(0)
    } else {
        0
    };
    let recursive_file_count = if args.counts {
        if is_dir {
            scan.true_file_counts.get(path).copied().unwrap_or(0)
        } else {
            1
        }
    } else {
        0
    };

    let mut node = Node {
        path: path.to_path_buf(),
        name: path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| ".".to_string()),
        metadata,
        children: Vec::new(),
        total_children_count: 0,
        omitted_size: 0,
        omitted_recursive_dir_count: 0,
        omitted_recursive_file_count: 0,
        omitted_dirs_count: 0,
        omitted_files_count: 0,
        is_dir,
        is_symlink,
        true_size,
        recursive_dir_count,
        recursive_file_count,
    };

    let is_git_dir = path
        .file_name()
        .map(|name| name == ".git")
        .unwrap_or(false);

    if is_dir && depth < args.max_depth && !(no_expand_git && is_git_dir) {
        if let Some(stubs) = scan.dir_children.get(path) {
            let mut entries = stubs
                .iter()
                .filter(|stub| !args.dirs_only || stub.file_type.is_dir())
                .collect::<Vec<_>>();

            let sort_config = args
                .sort
                .as_ref()
                .and_then(|v| match v.as_slice() {
                    [field, order] => Some((field.to_lowercase(), order.to_lowercase())),
                    _ => None,
                })
                .or_else(|| {
                    if args.sizes && !args.times && !args.counts {
                        Some(("size".to_string(), "desc".to_string()))
                    } else if args.times && !args.sizes && !args.counts {
                        Some(("time".to_string(), "desc".to_string()))
                    } else if args.counts && !args.sizes && !args.times {
                        Some(("count".to_string(), "desc".to_string()))
                    } else {
                        None
                    }
                });

            if let Some((field, order)) = sort_config {
                entries.sort_by(|a, b| {
                    let res = match field.as_str() {
                        "size" => {
                            let a_size = if args.sizes && a.file_type.is_dir() {
                                scan.true_sizes.get(&a.path).map(|v| *v).unwrap_or(0)
                            } else {
                                a.metadata.as_ref().map(|m| m.blocks() * 512).unwrap_or(0)
                            };
                            let b_size = if args.sizes && b.file_type.is_dir() {
                                scan.true_sizes.get(&b.path).map(|v| *v).unwrap_or(0)
                            } else {
                                b.metadata.as_ref().map(|m| m.blocks() * 512).unwrap_or(0)
                            };
                            a_size.cmp(&b_size)
                        }
                        "time" | "date" | "mtime" => {
                            let a_time = a.metadata.as_ref().and_then(|m| m.modified().ok());
                            let b_time = b.metadata.as_ref().and_then(|m| m.modified().ok());
                            a_time.cmp(&b_time)
                        }
                        "count" | "counts" => {
                            let a_count = if a.file_type.is_dir() {
                                scan.true_dir_counts.get(&a.path).copied().unwrap_or(0)
                                    + scan.true_file_counts.get(&a.path).copied().unwrap_or(0)
                            } else {
                                1
                            };
                            let b_count = if b.file_type.is_dir() {
                                scan.true_dir_counts.get(&b.path).copied().unwrap_or(0)
                                    + scan.true_file_counts.get(&b.path).copied().unwrap_or(0)
                            } else {
                                1
                            };
                            a_count.cmp(&b_count)
                        }
                        _ => cmp_name(&a.name, &b.name),
                    };
                    if order == "desc" { res.reverse() } else { res }
                });
            } else {
                // Plain default: type grouping (directories first), then
                // alphabetical by name within each group.
                entries.sort_by(|a, b| {
                    b.file_type
                        .is_dir()
                        .cmp(&a.file_type.is_dir())
                        .then_with(|| cmp_name(&a.name, &b.name))
                });
            }

            let filtered_out_count = if args.dirs_only {
                stubs.len().saturating_sub(entries.len())
            } else {
                0
            };
            let limit = if depth == 0 { entries.len() } else { entries.len().min(args.trunc) };
            let omitted_dirs_from_trunc = entries
                .iter()
                .skip(limit)
                .filter(|stub| stub.file_type.is_dir())
                .count();
            let omitted_files_from_trunc = entries
                .iter()
                .skip(limit)
                .filter(|stub| !stub.file_type.is_dir())
                .count();
            node.total_children_count = if args.dirs_only {
                entries.len()
            } else {
                entries.len() + filtered_out_count
            };
            node.omitted_dirs_count = omitted_dirs_from_trunc;
            node.omitted_files_count = if args.dirs_only {
                0
            } else {
                omitted_files_from_trunc + filtered_out_count
            };
            node.omitted_size = if args.sizes {
                let truncated_size: u64 = entries
                    .iter()
                    .skip(limit)
                    .map(|stub| {
                        if stub.file_type.is_dir() {
                            scan.true_sizes.get(&stub.path).copied().unwrap_or(0)
                        } else {
                            stub.metadata.as_ref().map(|m| m.blocks() * 512).unwrap_or(0)
                        }
                    })
                    .sum();
                let filtered_size: u64 = if args.dirs_only {
                    0
                } else {
                    stubs
                        .iter()
                        .filter(|stub| !stub.file_type.is_dir())
                        .map(|stub| stub.metadata.as_ref().map(|m| m.blocks() * 512).unwrap_or(0))
                        .sum()
                };
                truncated_size + filtered_size
            } else {
                0
            };
            node.omitted_recursive_dir_count = if args.counts {
                entries
                    .iter()
                    .skip(limit)
                    .map(|stub| {
                        if stub.file_type.is_dir() {
                            scan.true_dir_counts.get(&stub.path).copied().unwrap_or(0) + 1
                        } else {
                            0
                        }
                    })
                    .sum()
            } else {
                0
            };
            node.omitted_recursive_file_count = if args.counts {
                let truncated_files: u64 = entries
                    .iter()
                    .skip(limit)
                    .map(|stub| {
                        if stub.file_type.is_dir() {
                            scan.true_file_counts.get(&stub.path).copied().unwrap_or(0)
                        } else {
                            1
                        }
                    })
                    .sum();
                let filtered_files: u64 = 0;
                truncated_files + filtered_files
            } else {
                0
            };

            for stub in entries.into_iter().take(limit) {
                if let Some(child_node) = build_tree_from_cache(
                    &stub.path,
                    stub.metadata.clone(),
                    Some(stub.file_type),
                    Some(stub.is_symlink),
                    depth + 1,
                    args,
                    scan,
                    no_expand_git,
                ) {
                    node.children.push(child_node);
                }
            }
        }
    }

    Some(node)
}

fn print_node(
    node: &Node,
    depth: usize,
    prefixes: &[bool],
    args: &Args,
    lscolors: &LsColors,
    use_hyperlinks: bool,
    count_layout: CountColumnLayout,
    shown_dir_paths: &mut Vec<PathBuf>,
    shown_file_paths: &mut Vec<PathBuf>,
) {
    let child_count = node.children.len();
    let total_count = node.total_children_count;

    for (i, child) in node.children.iter().enumerate() {
        let is_last = i == child_count - 1 && total_count <= child_count;
        
        let color_reset = "\x1b[0m";
        let size_color = "\x1b[1;36m";
        let date_color = "\x1b[37m";

        if args.sizes {
            let display_size = child.true_size;
            let size_str = format_size(display_size);
            print!("{}{:>10}{} ", size_color, size_str, color_reset);
        }

        if args.times {
            let time_str = child.metadata.as_ref().map(|m| format_time(m)).unwrap_or_else(|| "-".to_string());
            print!("{}{:>16}{} ", date_color, time_str, color_reset);
        }

        if args.counts {
            print_recursive_count_pair(
                child.recursive_dir_count,
                child.recursive_file_count,
                count_layout,
            );
        }

        // Print prefix
        for &last in prefixes {
            if last {
                print!("    ");
            } else {
                print!("│   ");
            }
        }

        if is_last {
            print!("└── ");
        } else {
            print!("├── ");
        }

        // Styling
        let style = if child.is_symlink {
            lscolors.style_for_path(&child.path)
        } else if let Some(m) = child.metadata.as_ref() {
            lscolors.style_for_path_with_metadata(&child.path, Some(m))
        } else {
            lscolors.style_for_path(&child.path)
        };
        let ansi_style = style.map(|s| s.to_nu_ansi_term_style()).unwrap_or_default();

        let mut display_name = child.name.clone();
        if args.classify {
            if child.is_symlink {
                display_name.push('@');
            } else if child.is_dir {
                display_name.push('/');
            } else {
                let is_exec = child
                    .metadata
                    .as_ref()
                    .map(|md| md.permissions().mode() & 0o111 != 0)
                    .unwrap_or_else(|| is_executable_path(&child.path));
                if is_exec {
                    display_name.push('*');
                }
            }
        }

        let colored_name = ansi_style.paint(&display_name);

        if use_hyperlinks {
            if let Ok(abs_path) = std::fs::canonicalize(&child.path) {
                if let Ok(url) = Url::from_file_path(&abs_path) {
                    print!("\x1b]8;;{}\x1b\\{}\x1b]8;;\x1b\\", url, colored_name);
                } else {
                    print!("{}", colored_name);
                }
            } else {
                print!("{}", colored_name);
            }
        } else {
            print!("{}", colored_name);
        }
        println!();
        if args.cache_raw {
            if child.is_dir {
                shown_dir_paths.push(child.path.clone());
            } else {
                shown_file_paths.push(child.path.clone());
            }
        }

        if child.is_dir {
            let mut new_prefixes = prefixes.to_vec();
            new_prefixes.push(is_last);
            print_node(
                child,
                depth + 1,
                &new_prefixes,
                args,
                lscolors,
                use_hyperlinks,
                count_layout,
                shown_dir_paths,
                shown_file_paths,
            );
        }

    }

    if total_count > child_count && !args.hide_more_count {
        if args.sizes {
            let omitted_size_str = format_size(node.omitted_size);
            print!("{}{:>10}{} ", "\x1b[1;36m", omitted_size_str, "\x1b[0m");
        }
        if args.times {
            print!("{:>16} ", "");
        }
        if args.counts {
            print_recursive_count_pair(
                node.omitted_recursive_dir_count,
                node.omitted_recursive_file_count,
                count_layout,
            );
        }
        for &last in prefixes {
            if last {
                print!("    ");
            } else {
                print!("│   ");
            }
        }
        let mut omitted_parts = Vec::new();
        if node.omitted_dirs_count > 0 {
            let suffix = if node.omitted_dirs_count == 1 { "dir" } else { "dirs" };
            omitted_parts.push(format!("{} more {}", node.omitted_dirs_count, suffix));
        }
        if node.omitted_files_count > 0 {
            let suffix = if node.omitted_files_count == 1 { "file" } else { "files" };
            omitted_parts.push(format!("{} more {}", node.omitted_files_count, suffix));
        }
        if omitted_parts.is_empty() {
            println!("└── ... and {} more", total_count - child_count);
        } else {
            println!("└── ... and {}", omitted_parts.join(" "));
        }
    }
}
