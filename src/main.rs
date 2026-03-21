use chrono::{DateTime, Local};
use clap::Parser;
use dashmap::{DashMap, DashSet};
use jwalk::WalkDir;
use lscolors::LsColors;
use rayon::prelude::*;
use std::fs::Metadata;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
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

    /// Show file sizes
    #[arg(long, overrides_with = "sizes")]
    sizes: bool,

    /// Show file modification times
    #[arg(long, overrides_with = "times")]
    times: bool,

    /// Show true recursive directory sizes
    #[arg(long, overrides_with = "truesizes")]
    truesizes: bool,
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
    is_dir: bool,
    is_symlink: bool,
    true_size: u64,
}

fn main() {
    let args = Args::parse();
    let lscolors = LsColors::from_env().unwrap_or_default();
    let use_hyperlinks = args.hyperlinks;

    let root_path = args.path.as_ref().map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    
    let true_sizes = if args.truesizes {
        calculate_truesizes(&root_path, &args)
    } else {
        DashMap::new()
    };

    let root_metadata = if args.follow_links {
        root_path.metadata().ok()
    } else {
        root_path.symlink_metadata().ok()
    };
    let root_file_type = root_path.symlink_metadata().ok().map(|m| m.file_type());
    
    println!("{}", root_path.display());

    if let Some(root_node) = build_tree(&root_path, root_metadata, root_file_type, 0, &args, &true_sizes) {
        print_node(&root_node, 0, &Vec::new(), &args, &lscolors, use_hyperlinks);
    }
}

fn calculate_truesizes(root: &Path, args: &Args) -> DashMap<PathBuf, u64> {
    let dir_sizes = DashMap::new();
    let seen_inodes = DashSet::new();

    WalkDir::new(root)
        .skip_hidden(!args.all)
        .follow_links(args.follow_links)
        .into_iter()
        .par_bridge()
        .filter_map(|e| e.ok())
        .for_each(|entry| {
            let metadata = entry.metadata().ok();
            if let Some(m) = metadata {
                let dev = m.dev();
                let ino = m.ino();
                if seen_inodes.insert((dev, ino)) {
                    let size = m.blocks() * 512;
                    let path = entry.path();
                    let mut current = path.as_path();
                    while let Some(parent) = current.parent() {
                        *dir_sizes.entry(parent.to_path_buf()).or_insert(0) += size;
                        if parent == root { break; }
                        current = parent;
                    }
                }
            }
        });
    dir_sizes
}

fn build_tree(
    path: &Path,
    metadata: Option<Metadata>,
    file_type: Option<std::fs::FileType>,
    depth: usize,
    args: &Args,
    true_sizes: &DashMap<PathBuf, u64>,
) -> Option<Node> {
    let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
    let is_symlink = file_type.map(|ft| ft.is_symlink()).unwrap_or(false);

    let true_size = if args.truesizes && is_dir {
        true_sizes.get(path).map(|v| *v).unwrap_or(0)
    } else {
        metadata.as_ref().map(|m| m.blocks() * 512).unwrap_or(0)
    };

    let mut node = Node {
        path: path.to_path_buf(),
        name: path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| ".".to_string()),
        metadata,
        children: Vec::new(),
        total_children_count: 0,
        is_dir,
        is_symlink,
        true_size,
    };

    if is_dir && depth < args.max_depth {
        let mut entries: Vec<_> = WalkDir::new(path)
            .max_depth(1)
            .min_depth(1)
            .skip_hidden(!args.all)
            .follow_links(args.follow_links)
            .into_iter()
            .filter_map(|e| e.ok())
            .collect();

        // Sort: dirs first, then name
        entries.sort_by(|a, b| {
            let a_is_dir = a.file_type.is_dir();
            let b_is_dir = b.file_type.is_dir();
            if a_is_dir != b_is_dir {
                b_is_dir.cmp(&a_is_dir)
            } else {
                a.file_name.cmp(&b.file_name)
            }
        });

        node.total_children_count = entries.len();
        
        let limit = if depth == 0 {
            node.total_children_count
        } else {
            node.total_children_count.min(args.trunc)
        };

        for entry in entries.into_iter().take(limit) {
            let child_path = entry.path();
            let child_metadata = entry.metadata().ok();
            let child_file_type = Some(entry.file_type);
            if let Some(child_node) = build_tree(&child_path, child_metadata, child_file_type, depth + 1, args, true_sizes) {
                node.children.push(child_node);
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

        if args.sizes || args.truesizes {
            let display_size = if args.truesizes {
                child.true_size
            } else {
                child.metadata.as_ref().map(|m| m.len()).unwrap_or(0)
            };
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
        if args.sizes || args.truesizes {
            print!("{:>10} ", "");
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
