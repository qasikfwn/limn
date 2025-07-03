use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Mutex;
use std::thread;
use std::time::Instant;
use std::io;
use std::io::Read;
use std::io::Seek;
use std::panic;
use std::path::Path;
use std::path::PathBuf;

use limn::ExtractBuilder;
use limn::bundle::BundleFd;
use limn::file;
use limn::file::ExtractOptions;
use limn::file::Pool;
use limn::hash;
use limn::Oodle;
use limn::read::ChunkReader;

fn print_help() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
    println!("{}", env!("CARGO_PKG_AUTHORS"));
    println!();
    println!("limn extracts files from resource bundles used in Darktide.");
    println!();
    println!("limn uses oo2core_9_win64.dll to decompress the bundle. If it fails to load");
    println!("oo2core_9_win64.dll then copy it from the Darktide binaries folder next to limn.");
    println!();
    println!("Project home: {}", env!("CARGO_PKG_REPOSITORY"));
    println!();
    println!("USAGE:");
    println!("limn.exe [OPTIONS] <FILTER>");
    println!();
    println!("ARGS:");
    println!("    <FILTER>  Extract files with matching extension. Supports \"*\" as a wildcard.");
    println!();
    println!("OPTIONS:");
    println!("        --dump-hashes         Dump file extension and name hashes.");
    println!("        --dump-raw            Extract files without converting contents.");
    println!("    -i, --input <PATH>        Bundle or directory of bundles to extract.");
    println!("    -f, --filter <FILTER>     Only extract files with matching extension.");
}

struct Args {
    dump_hashes: bool,

    // always dump files raw instead of using crate::file::Extractor
    dump_raw: bool,

    // path to bundle OR directory of bundles
    target: PathBuf,

    filter_ext: Option<u64>,

    darktide_path: Option<PathBuf>,
}

fn parse_args() -> Args {
    let mut args = std::env::args_os();
    let _bin = args.next();

    let mut dump_hashes = false;
    let mut dump_raw = false;

    let mut target = None;
    let mut filter_ext = None;

    let mut num_args = 0;
    while let Some(arg) = args.next() {
        num_args += 1;

        let Some(opt) = arg.to_str() else {
            eprintln!("ERROR: invalid UTF-8 in arg {arg:?}");
            std::process::exit(1);
        };

        match opt {
            "--dump-hashes" => dump_hashes = true,

            "--dump-raw" => dump_raw = true,

            "-i" | "--input" => {
                let Some(param) = args.next() else {
                    eprintln!("ERROR: missing parameter to {}", opt);
                    std::process::exit(1);
                };
                target = Some(PathBuf::from(param));
            }

            "--help" => {
                print_help();
                std::process::exit(0);
            }

            filter => {
                let _owner;
                let ext = if opt == "-f" || opt == "--filter" {
                    let Some(param) = args.next() else {
                        eprintln!("ERROR: missing parameter to {}", opt);
                        std::process::exit(1);
                    };

                    _owner = param;
                    let Some(val) = _owner.to_str() else {
                        eprintln!("ERROR: invalid UTF-8 in parameter to {}", opt);
                        std::process::exit(1);
                    };

                    val
                } else if opt.starts_with("-") {
                    eprintln!("WARN: unknown option {}", opt);
                    continue;
                } else {
                    filter
                };

                if filter_ext.is_some() {
                    eprintln!("WARN: filter is already set, ignoring {ext:?}");
                    continue;
                }

                match ext {
                    "*" => (),
                    _ => filter_ext = Some(Some(hash::murmur_hash64a(ext.as_bytes(), 0))),
                }
            }
        }
    }

    if num_args == 0 {
        print_help();
        std::process::exit(0);
    }

    // hack to signal dupe/hash tracking
    if dump_hashes && filter_ext.is_none() {
        filter_ext = Some(Some(0));
    }

    let darktide_path = steam_find::get_steam_app(1361210).map(|app| app.path);
    let target = target.unwrap_or_else(|| {
        match &darktide_path {
            Ok(path) => path.join("bundle"),
            Err(e) => {
                eprintln!("Darktide steam installation was not found:\n{e:?}");
                std::process::exit(1);
            }
        }
    });

    Args {
        dump_hashes,
        dump_raw,

        target,
        filter_ext: filter_ext.flatten(),
        darktide_path: darktide_path.ok(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let Args {
        dump_hashes,
        dump_raw,

        target,
        filter_ext,
        darktide_path,
    } = parse_args();

    let dictionary = fs::read_to_string("dictionary.txt");

    let oodle = match load_oodle("oo2core_9_win64.dll", &target, darktide_path.as_ref())
        .or_else(|_| load_oodle("oo2core_8_win64.dll", &target, darktide_path.as_ref()))
    {
        Ok(oodle) => oodle,
        Err(e) => {
            eprintln!("oo2core_9_win64.dll could not be loaded");
            eprintln!("copy the dll from the Darktide binaries folder next to limn");
            eprintln!();
            return Err(Box::new(e));
        }
    };

    let mut builder = ExtractBuilder::new();
    builder.output(dump_hashes.then(|| "./out"))
        .oodle(oodle)
        .dump_hashes(dump_hashes)
        .dump_raw(dump_raw);
    if let Ok(dict) = dictionary {
        builder.dictionary(dict.lines());
    }

    let duplicates = Mutex::new(HashMap::new());
    let start = Instant::now();
    let options;
    let num_files = if let Ok(read_dir) = fs::read_dir(&target) {
        builder.input(target);
        options = builder.build()?;

        let mut bundles = Vec::new();
        for fd in read_dir {
            let fd = fd.as_ref().unwrap();
            let meta = fd.metadata().unwrap();
            if meta.is_file() {
                let path = fd.path();
                if path.extension().is_some() {
                    continue;
                }

                if let Some(bundle_hash) = bundle_hash_from(&path) {
                    bundles.push((path, bundle_hash));
                }
            }
        }

        let num_threads = thread::available_parallelism()
            .map(|i| i.get())
            .unwrap_or(0)
            .saturating_sub(1)
            .max(1);

        let mut dupes = duplicates.lock().unwrap();
        dupes.reserve(0x10000);
        drop(dupes);
        batch_threads(
            num_threads,
            &bundles,
            &duplicates,
            &options,
            filter_ext,
        )
    } else if let Ok(bundle) = File::open(&target) {
        builder.input(target.parent().unwrap().to_path_buf());
        options = builder.build()?;

        let bundle_hash = bundle_hash_from(&target);
        let mut buf = vec![0; 0x80000];
        let mut rdr = ChunkReader::new(&mut buf, bundle);
        Some(extract_bundle(
            &mut Pool::new(),
            &mut rdr,
            &mut Vec::new(),
            bundle_hash,
            &duplicates,
            &options,
            filter_ext,
        ).unwrap())
    } else {
        panic!("PATH argument was invalid");
    };

    println!();
    if let Some(num_files) = num_files {
        let ms = start.elapsed().as_millis();
        println!("DONE");
        println!("took {}.{}s", ms / 1000, ms % 1000);
        if !options.skip_extract() {
            println!("extracted {num_files} files");
        }

        if dump_hashes {
            let mut dupes = duplicates.into_inner()
                .unwrap()
                .into_iter()
                .map(|(hashes, _count)| hashes)
                .collect::<Vec<_>>();
            dupes.sort();
            let mut dupes = &dupes[..];
            if let Some(filter) = filter_ext.filter(|f| *f != 0) {
                let start = dupes.partition_point(|(ext, _)| *ext < filter);
                let end = dupes.partition_point(|(ext, _)| *ext <= filter);
                dupes = &dupes[start..end];
            }
            //let mut out = String::with_capacity((dupes.len() + 2) * (16 + 1 + 16 + 1));
            //out.push_str("name,extension\n");
            //for (ext, name) in &dupes {
            //    writeln!(&mut out, "{name:016x},{ext:016x}").unwrap();
            //}
            //fs::write("hashes.csv", &out)?;
            let mut bin = Vec::with_capacity(dupes.len() * 16);
            for (ext, name) in dupes {
                bin.extend_from_slice(&ext.to_le_bytes());
                bin.extend_from_slice(&name.to_le_bytes());
            }
            fs::write("hashes.bin", &bin)?;
            println!("{} file extension and name hashes written to \"hashes.bin\"", dupes.len());
        }
    } else {
        // TODO app exit code
        println!("did not finish due to errors");
    }

    Ok(())
}

fn batch_threads(
    num_threads: usize,
    bundles: &[(PathBuf, u64)],
    duplicates: &Mutex<HashMap<(u64, u64), u64>>,
    options: &ExtractOptions,
    filter: Option<u64>,
) -> Option<u32> {
    let bundle_index = Arc::new(AtomicUsize::new(0));
    let thread_errors = Arc::new(Mutex::new(Vec::with_capacity(num_threads)));

    let total = bundles.len();
    {
        let bundle_index = bundle_index.clone();
        let thread_errors = thread_errors.clone();
        panic::set_hook(Box::new(move |p| {
            let location = p.location().map(|l| l.to_string()).unwrap_or(String::new());
            let payload = if let Some(s) = p.payload().downcast_ref::<&str>() {
                s.to_string()
            } else if let Some(s) = p.payload().downcast_ref::<String>() {
                s.to_string()
            } else {
                String::new()
            };

            let mut thread_errors = thread_errors.lock().unwrap();
            if thread_errors.is_empty() {
                eprintln!("thread panic");
                bundle_index.store(total + num_threads, Ordering::Release);
            }
            thread_errors.push((location, payload));
        }));
    }

    thread::scope(|s| {
        let mut threads = Vec::with_capacity(num_threads);
        for _ in 0..num_threads {
            threads.push(s.spawn(|| {
                panic::catch_unwind(|| thread_work(
                    &bundles,
                    &bundle_index,
                    &duplicates,
                    &options,
                    filter,
                ))
            }));
        }

        let mut prev = (0, Instant::now());
        loop {
            thread::sleep(std::time::Duration::from_millis(1));

            let is_finished = threads.iter().all(|t| t.is_finished());
            if is_finished {
                if prev.0 < bundles.len()
                    && thread_errors.lock().unwrap().is_empty()
                {
                    println!("{}", bundles.len());
                }
                break;
            } else if prev.1.elapsed().as_millis() > 50 {
                let count = bundle_index.load(Ordering::Acquire)
                    .saturating_sub(num_threads);
                if count == prev.0 {
                    continue;
                }

                if count < total {
                    println!("{count}");
                }
                prev = (count, Instant::now());
            }
        }

        let threads = threads.into_iter().map(|t| t.join().unwrap()).collect::<Vec<_>>();
        let _ = panic::take_hook();

        if threads.iter().all(|t| t.is_ok()) {
            let mut num_files = 0;
            for thread in threads {
                num_files += thread.unwrap();
            }
            Some(num_files)
        } else {
            let thread_errors = thread_errors.lock().unwrap();
            if thread_errors.is_empty() {
                eprintln!("unknown thread panic");
            } else if thread_errors.len() == 1 {
                let (location, error) = &thread_errors[0];
                eprintln!();
                eprintln!("{location}");
                eprintln!("{error}");
            } else {
                let mut same = true;
                let first = &thread_errors[0].0;
                for (next, _) in &thread_errors[1..] {
                    if first != next {
                        same = false;
                        break;
                    }
                }

                eprintln!();
                if same {
                    eprintln!("  {first}");
                    for (_, error) in thread_errors.iter() {
                        eprintln!("{error}");
                    }
                } else {
                    eprintln!("  panics:");
                    for (location, error) in thread_errors.iter() {
                        eprintln!("{location}");
                        eprintln!("{error}");
                    }
                }
            }
            None
        }
    })
}

fn thread_work(
    bundles: &[(PathBuf, u64)],
    bundle_index: &AtomicUsize,
    duplicates: &Mutex<HashMap<(u64, u64), u64>>,
    options: &ExtractOptions,
    filter: Option<u64>,
) -> u32 {
    let mut pool = Pool::new();
    let mut buffer_reader = vec![0_u8; 0x80000];
    let mut bundle_buf = Vec::new();
    let mut num_files = 0;

    while let Some((path, bundle_hash)) =
        bundles.get(bundle_index.fetch_add(1, Ordering::AcqRel))
    {
        let bundle = File::open(&path).unwrap();
        let mut rdr = ChunkReader::new(&mut buffer_reader, bundle);
        num_files += extract_bundle(
            &mut pool,
            &mut rdr,
            &mut bundle_buf,
            Some(*bundle_hash),
            &duplicates,
            &options,
            filter,
        ).unwrap();
    }

    num_files
}

fn extract_bundle(
    pool: &mut Pool,
    mut rdr: impl Read + Seek,
    bundle_buf: &mut Vec<u8>,
    bundle_hash: Option<u64>,
    duplicates: &Mutex<HashMap<(u64, u64), u64>>,
    options: &ExtractOptions,
    filter: Option<u64>,
) -> io::Result<u32> {
    bundle_buf.clear();
    let mut bundle = BundleFd::new(bundle_hash, &mut rdr)?;
    let targets = if let Some(filter_ext) = filter {
        let mut targets = Vec::new();
        let mut dupes = duplicates.lock().unwrap();
        for file in bundle.index() {
            let key = (file.ext, file.name);
            let entry = dupes.entry(key).or_insert(0);
            *entry += 1;

            if *entry == 1 && file.ext == filter_ext {
                if options.skip_unknown()
                    && !options.contains_key(&file.name.into())
                {
                    continue;
                }
                targets.push((file.ext, file.name));
            }
        }
        drop(dupes);

        if targets.is_empty() {
            return Ok(0);
        } else {
            Some(targets)
        }
    } else {
        None
    };

    if options.skip_extract() {
        return Ok(targets.as_ref().map(|t| t.len() as u32).unwrap_or(0));
    }

    let mut targets = targets.as_ref().map(|t| &t[..]);
    let mut count = 0;
    let mut files = bundle.files(options.oodle(), bundle_buf);
    while let Ok(Some(file)) = files.next_file().map_err(|e| panic!("{:016x} - {}", bundle_hash.unwrap_or(0), e)) {
        if options.skip_unknown()
            && file.ext != /*lua*/0xa14e8dfa2cd117e2
            && !(filter == Some(file.ext) && file.ext == /*strings*/0x0d972bab10b40fd3)
            && !options.contains_key(&file.name.into())
        {
            continue;
        }

        if let Some(targets) = &mut targets {
            let (ext, name) = targets.first().unwrap();
            if *ext == file.ext && *name == file.name {
                (_, *targets) = targets.split_at(1);
            } else {
                continue;
            }
        }

        match file::extract(file, pool, options) {
            Ok(_wrote) => count += 1,
            Err(_e) => (),//eprintln!("{e}"),
        }

        if let Some(targets) = &targets {
            if targets.is_empty() {
                break;
            }
        }
    }

    Ok(count)
}

fn bundle_hash_from(path: &Path) -> Option<u64> {
    let name = path.file_stem()?;
    u64::from_str_radix(name.to_str()?, 16).ok()
}

fn load_oodle(
    name: &str,
    path: &Path,
    darktide_path: Option<&PathBuf>,
) -> Result<Oodle, io::Error> {
    match Oodle::load(name) {
        Ok(out) => Ok(out),
        Err(e) => {
            let oodle_path = format!("binaries/{name}");
            if let Some(oodle) = path.parent().map(|p| p.join(&oodle_path))
                .and_then(|p| Oodle::load(p).ok())
                .or_else(|| darktide_path.map(|path| path.join(&oodle_path))
                    .and_then(|p| Oodle::load(p).ok()))
            {
                Ok(oodle)
            } else {
                Err(e)
            }
        }
    }
}
