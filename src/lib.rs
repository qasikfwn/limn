use std::cell::Cell;
use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::panic::RefUnwindSafe;

pub mod bundle;
pub mod file;
use file::ExtractOptions;
pub mod hash;
use hash::MurmurHash;
use hash::MurmurHash32;
mod oodle;
pub use oodle::Oodle;
pub mod read;
mod scoped_fs;
use scoped_fs::FileOpen;
use scoped_fs::ScopedFs;

pub struct ExtractBuilder {
    input: Option<PathBuf>,
    output: Option<Box<dyn FileOpen>>,
    oodle: Option<Oodle>,
    dictionary: Option<HashMap<MurmurHash, String>>,
    dictionary_short: Option<HashMap<MurmurHash32, MurmurHash>>,

    dump_hashes: bool,
    dump_raw: bool,
}

impl ExtractBuilder {
    pub fn new() -> Self {
        Self {
            input: None,
            output: None,
            oodle: None,
            dictionary: None,
            dictionary_short: None,
            dump_hashes: false,
            dump_raw: false,
        }
    }

    pub fn input(
        &mut self,
        path: impl Into<PathBuf>,
    ) -> &mut Self {
        self.input = Some(path.into());
        self
    }

    pub fn output(
        &mut self,
        path: Option<impl AsRef<Path>>,
    ) -> &mut Self {
        let scoped_fs = if let Some(path) = path {
            ScopedFs::new(path.as_ref())
        } else {
            ScopedFs::new_null(&Path::new("./out"))
        };

        self.output = Some(Box::new(scoped_fs));
        self
    }

    pub fn output_custom(
        &mut self,
        cb: impl Fn(&str, &[u8]) + Send + Sync + RefUnwindSafe + 'static,
    ) -> &mut Self {
        self.output = Some(Box::new(BufOutput(cb)));
        self
    }

    pub fn oodle(&mut self, oodle: Oodle) -> &mut Self {
        self.oodle = Some(oodle);
        self
    }

    pub fn dictionary<T: Into<String>>(
        &mut self,
        keys: impl Iterator<Item = T>,
    ) -> &mut Self {
        let mut dict = HashMap::with_capacity(0x10000);
        let mut dict_short = HashMap::with_capacity(0x10000);
        for key in keys {
            let key = key.into();
            let hash = MurmurHash::new(&key);
            dict.insert(hash.clone(), key);
            dict_short.insert(hash.clone_short(), hash);
        }
        self.dictionary = Some(dict);
        self.dictionary_short = Some(dict_short);
        self
    }

    pub fn dump_hashes(&mut self, toggle: bool) -> &mut Self {
        self.dump_hashes = toggle;
        self
    }

    pub fn dump_raw(&mut self, toggle: bool) -> &mut Self {
        self.dump_raw = toggle;
        self
    }

    pub fn build(self) -> Result<ExtractOptions, &'static str> {
        let skip_unknown = self.dictionary.is_some();

        Ok(ExtractOptions {
            target: self.input.ok_or("missing input")?,
            out: self.output.ok_or("missing output")?,
            oodle: self.oodle.ok_or("missing oodle")?,
            dictionary: self.dictionary.unwrap_or_default(),
            dictionary_short: self.dictionary_short.unwrap_or_default(),
            skip_extract: self.dump_hashes,
            skip_unknown,
            as_blob: self.dump_raw,
        })
    }
}

struct BufOutput<T>(T)
where
    T: Fn(&str, &[u8]) + Send + Sync + RefUnwindSafe
;

impl<T> FileOpen for BufOutput<T>
where
    T: Fn(&str, &[u8]) + Send + Sync + RefUnwindSafe
{
    fn open(
        &self,
        path: &Path,
        scope: &mut dyn FnMut(&mut dyn io::Write) -> io::Result<u64>,
    ) -> io::Result<u64> {
        thread_local!(static BUFFER: Cell<Vec<u8>> = Cell::new(Vec::new()));

        let mut buffer = BUFFER.take();
        let res = scope(&mut buffer)?;
        self.0(
            path.to_str().unwrap(),
            &buffer,
        );
        BUFFER.set(buffer);
        Ok(res)
    }
}
