use std::fs;
use std::io;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;

pub(crate) trait FileOpen: Send + Sync + std::panic::RefUnwindSafe {
    fn open(
        &self,
        path: &Path,
        scope: &mut dyn FnMut(&mut dyn io::Write) -> io::Result<u64>,
    ) -> io::Result<u64>;
}

pub(crate) struct ScopedFs {
    root: PathBuf,
    is_null: bool,
}

impl ScopedFs {
    fn new_(root: &Path, is_null: bool) -> Self {
        let root = if is_null {
            root.to_path_buf()
        } else {
            fs::create_dir_all(root).unwrap();
            root.canonicalize().unwrap()
        };

        Self {
            root,
            is_null,
        }
    }

    pub(crate) fn new(root: &Path) -> Self {
        Self::new_(root, false)
    }

    #[allow(dead_code)]
    pub(crate) fn new_null(root: &Path) -> Self {
        Self::new_(root, true)
    }

    fn format_path(&self, path: &Path) -> io::Result<PathBuf> {
        let out = self.root.join(path);
        for part in out.components() {
            match part {
                //Component::RootDir => return false,
                Component::ParentDir => panic!(),
                _ => (),
            }
        }
        assert!(out.starts_with(&self.root));
        if !self.is_null {
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent)?;
            }
        }
        Ok(out)
    }
}

impl FileOpen for ScopedFs {
    fn open(
        &self,
        path: &Path,
        scope: &mut dyn FnMut(&mut dyn io::Write) -> io::Result<u64>,
    ) -> io::Result<u64> {
        if self.is_null {
            scope(&mut io::sink())
        } else {
            let path = self.format_path(path)?;
            scope(&mut fs::File::create(path)?)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[should_panic]
    fn scope() {
        let scope = ScopedFs::new(&Path::new("sandbox"));
        let _ = scope.open(Path::new("../target/test.bin"), &mut |_| Ok(0));
    }
}
