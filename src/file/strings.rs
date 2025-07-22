use std::fmt;
use super::*;

#[allow(dead_code)]
#[repr(u32)]
enum Language {
    ChineseTraditional,
    ChineseSimplified,
    English,
    English2,
    French,
    German,
    Italian,
    Japanese,
    Korean,
    Polish,
    Portuguese,
    Russian,
    Spanish,
}

impl Language {
    // Language codes can change between updates. The codes here are best effort
    // and are likely incorrect.
    //
    // TODO find where language ids map to codes at runtime
    fn from_code(code: u32) -> Option<Self> {
        debug_assert!(code == 0 || code.is_power_of_two());
        Some(match code {
            0    => Self::English,
            //1    => Self::Polish,
            //2    => Self::Japanese,
            //4    => Self::Spanish,
            //8    => Self::English2,
            //16   => Self::ChineseTraditional,
            //32   => Self::Portuguese,
            //64   => Self::German,
            //128  => Self::Korean,
            //256  => Self::Russian,
            //512  => Self::Italian,
            //1024 => Self::ChineseSimplified,
            //2048 => Self::French,
            _ => return None,
        })
    }
}

impl fmt::Display for Language {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match *self {
            Self::ChineseTraditional => "chinese_traditional",
            Self::ChineseSimplified  => "chinese_simplified",
            Self::English            => "english",
            Self::English2           => "english2",
            Self::French             => "french",
            Self::German             => "german",
            Self::Italian            => "italian",
            Self::Japanese           => "japanese",
            Self::Korean             => "korean",
            Self::Polish             => "polish",
            Self::Portuguese         => "portuguese",
            Self::Russian            => "russian",
            Self::Spanish            => "spanish",
        })
    }
}

pub(crate) struct StringsParser;

impl Extractor for StringsParser {
    fn extract(
        &self,
        entry: &mut Entry<'_, '_>,
        file_path: &Path,
        shared: &mut [u8],
        shared_flex: &mut Vec<u8>,
        options: &ExtractOptions,
    ) -> io::Result<u64> {
        let mut wrote = 0;
        let mut variant_i = 0;
        while let Some(variant) = entry.variants().get(variant_i) {
            let mut shared = &mut shared[..];
            variant_i += 1;
            let kind = variant.kind;
            let variant_size = variant.body_size;

            let _unk = entry.read_u32::<LE>()?;
            //assert_eq!(_unk, 0x3e85f3ae);
            let num_items = entry.read_u32::<LE>()?;
            let mut offset = 8;
            let size_needed = num_items as usize * 8;
            assert!(shared.len() > (size_needed + 0x1000), "{}, {size_needed}", shared.len());
            let (hashes, buffer) = shared.split_at_mut(size_needed);
            let mut hashes_into = &mut hashes[..];
            let mut last = None;
            for _ in 0..num_items {
                let short_hash = entry.read_u32::<LE>()?;
                let string_offset = entry.read_u32::<LE>()?;
                if let Some((last_hash, last_offset)) = last {
                    hashes_into.write_u32::<LE>(last_hash)?;
                    // store length
                    hashes_into.write_u32::<LE>(string_offset - last_offset)?;
                }
                last = Some((short_hash, string_offset));
                offset += 8;
            }
            if let Some((last_hash, last_offset)) = last {
                hashes_into.write_u32::<LE>(last_hash)?;
                hashes_into.write_u32::<LE>(variant_size - last_offset)?;
            }

            let mut hashes = &hashes[..];
            shared_flex.clear();
            let mut is_trailing = false;
            write!(shared_flex, "{{")?;
            for _ in 0..num_items {
                let short_hash = hashes.read_u32::<LE>()?;
                let string_len = hashes.read_u32::<LE>()? as usize;
                let do_print = if let Some(hash) = options.dictionary_short.get(&short_hash.into()) {
                    let key = options.dictionary.get(hash).unwrap();
                    if is_trailing {
                        write!(shared_flex, ",")?;
                    }
                    is_trailing = true;
                    write!(shared_flex, "{key:?}:\"")?;
                    true
                } else if !options.skip_unknown {
                    if is_trailing {
                        write!(shared_flex, ",")?;
                    }
                    is_trailing = true;
                    write!(shared_flex, "\"{short_hash:08x}\":\"")?;
                    true
                } else {
                    false
                };

                assert!(buffer.len() >= string_len);
                entry.read_exact(&mut buffer[..string_len])?;
                assert_eq!(0, buffer[string_len - 1]);
                if do_print {
                    shared_flex.reserve(string_len * 2);
                    for c in std::str::from_utf8(&buffer[..string_len - 2]).unwrap().chars() {
                        match c {
                            '\0' => {
                                // characters with a nul before the end have
                                // trailing "[Narrative]" or "[Dev]" text

                                break;
                            }
                            '\t'
                            | '\n'
                            | '\r'
                            | '"' => {
                                write!(shared_flex, "\\{}", match c {
                                    '\t' => 't',
                                    '\n' => 'n',
                                    '\r' => 'r',
                                    '"'  => '"',
                                    _ => unreachable!(),
                                })?;
                            }
                            _ => {
                                write!(shared_flex, "{c}")?;
                            }
                        }
                    }
                    write!(shared_flex, "\"")?;
                }
                offset += string_len;
            }

            assert_eq!(offset, variant_size as usize);
            write!(shared_flex, "}}")?;

            let lang = if let Some(lang) = Language::from_code(kind) {
                write_help!(&mut shared, "{lang}")
            } else {
                write_help!(&mut shared, "{kind:04x}")
            };

            let stem = file_path.file_stem().unwrap().to_str().unwrap();
            let file = write_help!(&mut shared, "{stem}.{lang}");
            let parent = file_path.parent().unwrap();
            let path = path_concat(parent, &mut shared, file, Some("json"));

            wrote += options.write(path, &shared_flex)?;
        }

        Ok(wrote)
    }
}
