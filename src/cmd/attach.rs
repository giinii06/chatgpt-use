//! Files sent WITH the message instead of inlined into it.
//!
//! `--file` reads text into the prompt, which is right for a diff or a log and
//! wrong for a tree: the composer insert is chunked, the text counts against
//! the model's context, and the message stops being readable. `--attach` puts
//! the bytes in ChatGPT's attachment slot instead.
//!
//! A directory has to become one file first, so it is zipped here, before the
//! browser is touched. The archive lives in a temp directory this value owns
//! and removes on drop.

use anyhow::{bail, Context, Result};
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

/// Total bytes a directory may hold before zipping. The generated archive is
/// handed to a chrome-use subprocess with no timeout of its own, and a wedged
/// session costs a reload, so refuse well before a pathological tree.
const MAX_DIR_BYTES: u64 = 200 * 1024 * 1024;

/// Attachment paths ready to upload. `temp` is the directory holding archives
/// this process created; dropping the value removes it.
#[derive(Debug)]
pub struct Prepared {
    files: Vec<PathBuf>,
    temp: Option<PathBuf>,
}

impl Prepared {
    pub fn paths(&self) -> &[PathBuf] {
        &self.files
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        if let Some(dir) = &self.temp {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

/// Turn each requested path into one uploadable file: a file stays as it is, a
/// directory becomes `<name>.zip` in a per-run temp directory.
pub fn prepare(paths: &[String]) -> Result<Prepared> {
    let mut files = Vec::new();
    let mut temp: Option<PathBuf> = None;
    for raw in paths {
        let path = PathBuf::from(raw);
        let meta =
            fs::metadata(&path).with_context(|| format!("attachment not found: {raw}"))?;
        if meta.is_file() {
            files.push(path);
            continue;
        }
        if !meta.is_dir() {
            bail!("attachment is neither a file nor a directory: {raw}");
        }
        let dir = match &temp {
            Some(d) => d.clone(),
            None => {
                let d = temp_dir();
                fs::create_dir_all(&d)
                    .with_context(|| format!("could not create {}", d.display()))?;
                temp = Some(d.clone());
                d
            }
        };
        let name = path
            .file_name()
            .and_then(OsStr::to_str)
            .filter(|n| !n.is_empty())
            .unwrap_or("attachment");
        let archive = dir.join(format!("{name}.zip"));
        zip_dir(&path, &archive)?;
        files.push(archive);
    }
    Ok(Prepared { files, temp })
}

/// Unique per run: a pid alone can repeat after a crash left its archive.
fn temp_dir() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "chatgpt-use-attach-{}-{nanos}",
        std::process::id()
    ))
}

fn zip_dir(src: &Path, dest: &Path) -> Result<()> {
    let bytes = dir_bytes(src)?;
    if bytes > MAX_DIR_BYTES {
        bail!(
            "{} holds {} bytes; the limit for a generated archive is {} bytes — \
             zip it yourself and attach the archive",
            src.display(),
            bytes,
            MAX_DIR_BYTES
        );
    }

    let file = File::create(dest)
        .with_context(|| format!("could not create {}", dest.display()))?;
    let mut zip = ZipWriter::new(file);
    let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);

    for entry in WalkDir::new(src).sort_by_file_name() {
        let entry = entry.with_context(|| format!("walking {}", src.display()))?;
        let rel = entry.path().strip_prefix(src)?;
        if rel.as_os_str().is_empty() {
            continue;
        }
        // A project root's .git is usually most of its bytes and none of its
        // meaning. Only the top level: a nested .git in a vendored tree is
        // still part of that tree.
        if rel.components().next() == Some(Component::Normal(OsStr::new(".git"))) {
            continue;
        }
        let name = rel.to_string_lossy().replace('\\', "/");
        if entry.file_type().is_dir() {
            zip.add_directory(name, opts)?;
        } else if entry.file_type().is_file() {
            zip.start_file(name, opts)?;
            let mut f = File::open(entry.path())
                .with_context(|| format!("could not read {}", entry.path().display()))?;
            io::copy(&mut f, &mut zip)?;
        }
        // Symlinks are skipped: the archive must not point outside the tree.
    }
    zip.finish()?;
    Ok(())
}

fn dir_bytes(src: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in WalkDir::new(src) {
        let entry = entry.with_context(|| format!("walking {}", src.display()))?;
        let rel = entry.path().strip_prefix(src)?;
        if rel.components().next() == Some(Component::Normal(OsStr::new(".git"))) {
            continue;
        }
        if entry.file_type().is_file() {
            total += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zip::ZipArchive;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cgu-attach-test-{tag}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn a_directory_becomes_one_zip_with_its_tree() {
        let root = scratch("zip");
        fs::create_dir_all(root.join("src/nested")).unwrap();
        fs::create_dir_all(root.join(".git")).unwrap();
        fs::write(root.join("src/main.rs"), "fn main() {}").unwrap();
        fs::write(root.join("src/nested/lib.rs"), "pub fn f() {}").unwrap();
        fs::write(root.join(".git/config"), "[core]").unwrap();

        let prepared = prepare(&[root.to_string_lossy().into_owned()]).unwrap();
        assert_eq!(prepared.paths().len(), 1);
        let archive = &prepared.paths()[0];
        assert_eq!(archive.extension().and_then(OsStr::to_str), Some("zip"));

        let mut zip = ZipArchive::new(File::open(archive).unwrap()).unwrap();
        let mut names: Vec<String> = (0..zip.len())
            .map(|i| zip.by_index(i).unwrap().name().to_string())
            .collect();
        names.sort();
        assert!(names.contains(&"src/main.rs".to_string()), "{names:?}");
        assert!(names.contains(&"src/nested/lib.rs".to_string()), "{names:?}");
        assert!(!names.iter().any(|n| n.starts_with(".git")), "{names:?}");

        let temp = prepared.temp.clone().unwrap();
        drop(prepared);
        assert!(!temp.exists(), "the archive directory outlived Prepared");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_plain_file_passes_through_untouched() {
        let root = scratch("file");
        let file = root.join("change.patch");
        fs::write(&file, "diff --git a/x b/x").unwrap();

        let prepared = prepare(&[file.to_string_lossy().into_owned()]).unwrap();
        assert_eq!(prepared.paths(), std::slice::from_ref(&file));
        assert!(prepared.temp.is_none());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_missing_path_is_named() {
        let err = prepare(&["/nonexistent/cgu-attach".into()]).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/cgu-attach"), "{err}");
    }
}
