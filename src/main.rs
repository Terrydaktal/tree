use chrono::{DateTime, Local};
use clap::Parser;
use dashmap::{DashMap, DashSet};
use jwalk::WalkDir;
use lscolors::LsColors;
use rustc_hash::FxBuildHasher;
use std::ffi::OsString;
use std::collections::HashMap;
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

    /// Show hidden files
    #[arg(short = 'a', overrides_with = "all")]
    all: bool,

    /// Max depth to display
    #[arg(short = 'L', default_value = "100", overrides_with = "max_depth")]
    max_depth: usize,

    /// Classify (add / for dirs, * for executables)
    #[arg(short = 'F', overrides_with = "classify")]
    classify: bool,

    /// Truncate depth 2+ entries to this value
    #[arg(short = 'T', long, default_value = "10", overrides_with = "trunc")]
    trunc: usize,

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

    /// Alias for -st (show sizes and times)
    #[arg(short = 'l', overrides_with = "long_listing")]
    long_listing: bool,

    /// Reverse the final displayed output lines
    #[arg(short = 'r', long, overrides_with = "reverse")]
    reverse: bool,

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

struct Node {
    path: PathBuf,
    name: String,
    metadata: Option<Metadata>,
    children: Vec<Node>,
    total_children_count: usize,
    omitted_size: u64,
    is_dir: bool,
    is_symlink: bool,
    true_size: u64,
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
}

fn main() {
    let mut args = Args::parse();
    const REVERSE_ENV_KEY: &str = "TREE_INTERNAL_REVERSE";

    if args.long_listing {
        args.sizes = true;
        args.times = true;
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
        let prefix_base = (if args.sizes { 11 } else { 0 }) + (if args.times { 17 } else { 0 });

        let connector_index = |line: &str| -> Option<usize> {
            line.find("├── ")
                .or_else(|| line.find("└── "))
                .or_else(|| line.find("┌── "))
        };

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

        // Root-level rows should still terminate at the final displayed row.
        let root_indices: Vec<usize> = depths
            .iter()
            .enumerate()
            .filter_map(|(idx, depth)| if *depth == Some(0) { Some(idx) } else { None })
            .collect();
        for (pos, &idx) in root_indices.iter().enumerate() {
            let conn = if root_indices.len() == 1 || pos + 1 == root_indices.len() {
                "└── "
            } else if pos == 0 {
                "┌── "
            } else {
                "├── "
            };
            set_conn(&mut reversed[idx], conn);
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
    
    // PHASE 1: Single Unified Parallel Scan
    let scan = perform_unified_scan(&root_abs, &args);

    let root_metadata = if args.follow_links {
        root_abs.metadata().ok()
    } else {
        root_abs.symlink_metadata().ok()
    };
    let root_file_type = root_abs.symlink_metadata().ok().map(|m| m.file_type());
    
    println!("{}", root_path.display());

    // PHASE 2: In-Memory Tree Build (Zero Disk IO)
    if let Some(root_node) = build_tree_from_cache(&root_abs, root_metadata, root_file_type, None, 0, &args, &scan) {
        print_node(&root_node, 0, &Vec::new(), &args, &lscolors, use_hyperlinks);
    }
}

fn perform_unified_scan(root: &Path, args: &Args) -> ScanResult {
    let dir_children = Arc::new(DashMap::with_hasher(FxBuildHasher::default()));
    let dir_local_sizes = Arc::new(DashMap::with_hasher(FxBuildHasher::default()));
    let seen_inodes = Arc::new(DashSet::with_hasher(FxBuildHasher::default()));
    let seen_dir_inodes = Arc::new(DashSet::with_hasher(FxBuildHasher::default()));

    // Seed root size
    if let Ok(m) = root.symlink_metadata() {
        seen_inodes.insert((m.dev(), m.ino()));
        dir_local_sizes.insert(root.to_path_buf(), m.blocks() * 512);
    }

    let dc = Arc::clone(&dir_children);
    let ds = Arc::clone(&dir_local_sizes);
    let si = Arc::clone(&seen_inodes);
    let sdi = Arc::clone(&seen_dir_inodes);
    let follow_links = args.follow_links;

    WalkDir::new(root)
        .skip_hidden(!args.all)
        .follow_links(args.follow_links)
        .parallelism(jwalk::Parallelism::RayonNewPool(args.threads))
        .process_read_dir(move |_depth, path, _state, children| {
            let mut local_sum = 0u64;
            let mut stubs = Vec::with_capacity(children.len());

            for entry_res in children.iter_mut().filter_map(|e| e.as_mut().ok()) {
                let m = entry_res.metadata().ok();

                // Avoid re-descending into duplicate directory inodes reached via different
                // symlink paths when --follow-links is enabled.
                if follow_links && entry_res.file_type.is_dir() {
                    if let Some(ref metadata) = m {
                        if !sdi.insert((metadata.dev(), metadata.ino())) {
                            entry_res.read_children_path = None;
                        }
                    }
                }

                if let Some(ref metadata) = m {
                    if si.insert((metadata.dev(), metadata.ino())) {
                        local_sum += metadata.blocks() * 512;
                    }
                }
                
                stubs.push(EntryStub {
                    name: entry_res.file_name.to_string_lossy().into_owned(),
                    path: entry_res.path(),
                    metadata: m,
                    file_type: entry_res.file_type,
                    is_symlink: entry_res.path_is_symlink(),
                });
            }

            if !stubs.is_empty() {
                dc.insert(path.to_path_buf(), stubs);
            }
            // Track every scanned directory so upward aggregation can propagate
            // through directories that have 0 local blocks but non-zero descendants.
            *ds.entry(path.to_path_buf()).or_insert(0) += local_sum;
        })
        .into_iter()
        .for_each(|_| {});

    // Finalize sizes (Upward Aggregation)
    let mut true_sizes: HashMap<PathBuf, u64, FxBuildHasher> = HashMap::with_hasher(FxBuildHasher::default());
    for entry in dir_local_sizes.iter() {
        true_sizes.insert(entry.key().clone(), *entry.value());
    }

    let mut paths: Vec<_> = true_sizes.keys().cloned().collect();
    paths.sort_unstable_by_key(|p| std::cmp::Reverse(p.components().count()));

    for path in paths {
        if let Some(parent) = path.parent() {
            if parent.starts_with(root) || parent == root {
                if let Some(&size) = true_sizes.get(&path) {
                    if path != root {
                        *true_sizes.entry(parent.to_path_buf()).or_insert(0) += size;
                    }
                }
            }
        }
    }

    let mut final_children = HashMap::with_hasher(FxBuildHasher::default());
    // Use Arc's internal DashMap without cloning if possible, or just iterate.
    for entry in dir_children.iter() {
        final_children.insert(entry.key().clone(), entry.value().clone());
    }

    ScanResult {
        dir_children: final_children,
        true_sizes,
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
) -> Option<Node> {
    let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
    let is_symlink = is_symlink_hint.unwrap_or_else(|| file_type.map(|ft| ft.is_symlink()).unwrap_or(false));

    let true_size = if args.sizes && is_dir {
        scan.true_sizes.get(path).map(|v| *v).unwrap_or(0)
    } else {
        metadata.as_ref().map(|m| m.blocks() * 512).unwrap_or(0)
    };

    let mut node = Node {
        path: path.to_path_buf(),
        name: path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| ".".to_string()),
        metadata,
        children: Vec::new(),
        total_children_count: 0,
        omitted_size: 0,
        is_dir,
        is_symlink,
        true_size,
    };

    if is_dir && depth < args.max_depth {
        if let Some(stubs) = scan.dir_children.get(path) {
            let mut entries = stubs.iter().collect::<Vec<_>>();

            let sort_config = args
                .sort
                .as_ref()
                .and_then(|v| match v.as_slice() {
                    [field, order] => Some((field.to_lowercase(), order.to_lowercase())),
                    _ => None,
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
                        _ => a.name.cmp(&b.name),
                    };
                    if order == "desc" { res.reverse() } else { res }
                });
            } else {
                entries.sort_by(|a, b| a.name.cmp(&b.name));
            }

            node.total_children_count = entries.len();
            let limit = if depth == 0 { node.total_children_count } else { node.total_children_count.min(args.trunc) };
            node.omitted_size = entries
                .iter()
                .skip(limit)
                .map(|stub| {
                    if args.sizes {
                        if stub.file_type.is_dir() {
                            scan.true_sizes.get(&stub.path).copied().unwrap_or(0)
                        } else {
                            stub.metadata.as_ref().map(|m| m.blocks() * 512).unwrap_or(0)
                        }
                    } else {
                        stub.metadata.as_ref().map(|m| m.len()).unwrap_or(0)
                    }
                })
                .sum();

            for stub in entries.into_iter().take(limit) {
                if let Some(child_node) = build_tree_from_cache(
                    &stub.path,
                    stub.metadata.clone(),
                    Some(stub.file_type),
                    Some(stub.is_symlink),
                    depth + 1,
                    args,
                    scan,
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
        } else {
            child.metadata.as_ref().and_then(|m| lscolors.style_for_path_with_metadata(&child.path, Some(m)))
        };
        let ansi_style = style.map(|s| s.to_nu_ansi_term_style()).unwrap_or_default();

        let mut display_name = child.name.clone();
        if args.classify {
            if child.is_symlink {
                display_name.push('@');
            } else if child.is_dir {
                display_name.push('/');
            } else if let Some(md) = &child.metadata {
                if md.permissions().mode() & 0o111 != 0 {
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

        if child.is_dir {
            let mut new_prefixes = prefixes.to_vec();
            new_prefixes.push(is_last);
            print_node(child, depth + 1, &new_prefixes, args, lscolors, use_hyperlinks);
        }

    }

    if total_count > child_count {
        if args.sizes {
            let omitted_size_str = format_size(node.omitted_size);
            print!("{}{:>10}{} ", "\x1b[1;36m", omitted_size_str, "\x1b[0m");
        }
        if args.times {
            print!("{:>16} ", "");
        }
        for &last in prefixes {
            if last {
                print!("    ");
            } else {
                print!("│   ");
            }
        }
        println!("└── ... and {} more", total_count - child_count);
    }
}
