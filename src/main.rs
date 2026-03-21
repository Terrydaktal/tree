use clap::Parser;
use jwalk::WalkDir;
use lscolors::LsColors;
use std::fs::Metadata;
use std::os::unix::fs::PermissionsExt;
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
}

struct Node {
    path: PathBuf,
    name: String,
    metadata: Option<Metadata>,
    children: Vec<Node>,
    total_children_count: usize,
    is_dir: bool,
    is_symlink: bool,
}

fn main() {
    let args = Args::parse();
    let lscolors = LsColors::from_env().unwrap_or_default();
    let use_hyperlinks = args.hyperlinks;

    let root_path = args.path.as_ref().map(PathBuf::from).unwrap_or_else(|| PathBuf::from("."));
    let root_metadata = if args.follow_links {
        root_path.metadata().ok()
    } else {
        root_path.symlink_metadata().ok()
    };
    
    println!("{}", root_path.display());

    if let Some(root_node) = build_tree(&root_path, root_metadata, 0, &args) {
        print_node(&root_node, 0, &Vec::new(), &args, &lscolors, use_hyperlinks);
    }
}

fn build_tree(path: &Path, metadata: Option<Metadata>, depth: usize, args: &Args) -> Option<Node> {
    let is_dir = metadata.as_ref().map(|m| m.is_dir()).unwrap_or(false);
    let is_symlink = path.symlink_metadata().map(|m| m.file_type().is_symlink()).unwrap_or(false);

    let mut node = Node {
        path: path.to_path_buf(),
        name: path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_else(|| ".".to_string()),
        metadata,
        children: Vec::new(),
        total_children_count: 0,
        is_dir,
        is_symlink,
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
            if let Some(child_node) = build_tree(&child_path, child_metadata, depth + 1, args) {
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
        let style = child.metadata.as_ref().and_then(|m| lscolors.style_for_path_with_metadata(&child.path, Some(m)));
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
