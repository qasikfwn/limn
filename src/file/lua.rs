use super::*;

pub(crate) struct LuaParser;

impl Extractor for LuaParser {
    fn extract(
        &self,
        mut entry: &mut Entry<'_, '_>,
        _file_path: &Path,
        shared: &mut [u8],
        shared_flex: &mut Vec<u8>,
        options: &ExtractOptions,
    ) -> io::Result<u64> {
        let variants = entry.variants();
        shared_flex.clear();

        assert_eq!(1, variants.len());

        let body_size = variants[0].body_size;
        let _ = entry.read_u32::<LE>();
        let mut file_len = entry.read_u32::<LE>().unwrap();
        let _ = entry.read_u32::<LE>();

        let mut header = entry.read_u32::<LE>().unwrap();
        let has_source = if header == 2 {
            let _ = entry.read_u32::<LE>();
            let _ = entry.read_u32::<LE>();
            header = entry.read_u32::<LE>().unwrap();
            true
        } else {
            file_len = body_size;
            false
        };
        let file_len = file_len as u64;
        assert!(header == 38423579 || header == 2186495515, "{:016x}.lua has unexpected header {header:08x}", entry.name);

        assert_eq!(entry.read_u8().unwrap(), 0);
        let path_len = leb128::read::unsigned(&mut entry).unwrap();
        assert_eq!(entry.read_u8().unwrap(), b'@');
        let len = path_len as usize - 1;

        // always write valid LuaJIT header
        shared_flex.write_u32::<LE>(38423579).unwrap();
        shared_flex.write_u8(0).unwrap();
        leb128::write::unsigned(&mut *shared_flex, path_len).unwrap();
        shared_flex.write_u8(b'@').unwrap();

        let slice;
        (slice, _) = shared.split_at_mut(len);
        for b in slice.iter_mut() {
            let c = entry.read_u8().unwrap();
            *b = c;
            shared_flex.write_u8(c).unwrap();
        }
        let lua_path = std::str::from_utf8(&slice).unwrap();

        if has_source && options.config.contains("extract-lua-source") {
            io::copy(&mut entry.take(file_len - shared_flex.len() as u64), &mut io::sink()).unwrap();

            shared_flex.clear();
            io::copy(&mut entry, &mut *shared_flex).unwrap();
        } else {
            io::copy(&mut entry.take(file_len - shared_flex.len() as u64), &mut *shared_flex).unwrap();
        }

        options.write(lua_path.as_ref(), &shared_flex)
    }
}
