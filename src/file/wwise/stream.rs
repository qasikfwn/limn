// Converts Wwise Vorbis streams to Ogg Vorbis.
// Other audio types are unsupported.
//
// References:
// - ww2ogg (github.com/hcs64/ww2ogg)
// - revorb (yirkha.fud.cz/progs/foobar2000/revorb.cpp)
// - Vorbis (github.com/xiph/vorbis)
// - vgmstream (github.com/vgmstream/vgmstream)
// - Filediver (github.com/xypwn/filediver)
// - granulepos repair tool (lists.xiph.org/pipermail/vorbis-dev/2010-November/020173.html)
use crate::read::ChunkReader;
use crate::file::*;

const STREAM_MAGIC: u32 = 0x46464952; //RIFF
const STREAM_KIND: u32 = 0x45564157; //WAVE
const FMT_MAGIC: u32 = 0x20746d66; //"fmt "
const DATA_MAGIC: u32 = 0x61746164; //data

static AOTUV603_CODEBOOK: Codebook<'static> = Codebook::new(include_bytes!("packed_codebooks_aoTuV_603.bin"));
static CRC_TREMOR_LOWMEM: [u32; 256] = {
    let data = include_bytes!("crc_tremor_lowmem.bin");
    assert!(256 * 4 == data.len());
    let mut lookup = [0; 256];
    let mut i = 0;
    while i < data.len() {
        let j = i / 4;
        let mut buf = [0; 4];
        let (_, mut d) = data.split_at(i);
        (d, _) = d.split_at(4);
        buf.copy_from_slice(d);
        lookup[j] = u32::from_le_bytes(buf);
        i += 4;
    }
    lookup
};

fn checksum(data: &[u8]) -> u32 {
    let lookup = &CRC_TREMOR_LOWMEM;
    let mut c = 0;
    for b in data {
        c = (c << 8) ^ lookup[(((c >> 24) & 0xff) ^ *b as u32) as usize];
    }
    c
}

fn msb(i: u32) -> u32 {
    u32::BITS - i.leading_zeros()
}

struct Codebook<'a> {
    data: &'a [u8],
    offsets: &'a [u8],
}

impl<'a> Codebook<'a> {
    const fn new(data: &'a [u8]) -> Self {
        let (_, offset) = data.split_at(data.len() - 4);
        let mut buf = [0; 4];
        buf.copy_from_slice(offset);
        let offset = u32::from_le_bytes(buf);
        let (data, offsets) = data.split_at(offset as usize);
        let (offsets, _) = offsets.split_at(offsets.len() - 4);
        Self {
            data,
            offsets,
        }
    }

    fn data(&self, id: u32) -> Option<&[u8]> {
        let o = id as usize * 4;
        let start = &self.offsets[o..o + 4];
        let start = u32::from_le_bytes(<[u8; 4]>::try_from(start).unwrap());
        let end = &self.offsets[o + 4..o + 8];
        let end = u32::from_le_bytes(<[u8; 4]>::try_from(end).unwrap());
        self.data.get(start as usize..end as usize)
    }
}

struct BitIo<'a> {
    reader: &'a mut dyn Read,
    roffset: u32,
    rtmp: u32,

    writer: &'a mut Vec<u8>,
    woffset: u32,
}

impl<'a> BitIo<'a> {
    fn new(
        reader: &'a mut dyn Read,
        writer: &'a mut Vec<u8>,
    ) -> Self {
        Self {
            reader,
            roffset: 0,
            rtmp: 0,

            woffset: u32::try_from(writer.len() * 8).unwrap(),
            writer,
        }
    }

    fn mask(&self, bits: u32) -> u32 {
        match bits.min(7) {
            1 => 0b0000001,
            2 => 0b0000011,
            3 => 0b0000111,
            4 => 0b0001111,
            5 => 0b0011111,
            6 => 0b0111111,
            7 => 0b1111111,
            _ => unreachable!(),
        }
    }

    fn read(&mut self, bits: u32) -> u32 {
        assert!(bits != 0 && bits <= 32);

        let mut offset = 0;
        let mut out = 0;
        let roffset = self.roffset % 8;
        if roffset > 0 {
            let mask = self.mask(bits.min(7));
            out = (self.rtmp >> roffset) & mask;
            offset = (8 - roffset).min(bits);
        }

        let diff = bits - offset;
        if diff == 32 {
            assert!(offset == 0);
            out |= self.reader.read_u32::<LE>().unwrap();
            offset += 32;
        } else if diff >= 24 {
            out |= self.reader.read_u24::<LE>().unwrap() << offset;
            offset += 24;
        } else if diff >= 16 {
            out |= u32::from(self.reader.read_u16::<LE>().unwrap()) << offset;
            offset += 16;
        } else if diff >= 8 {
            out |= u32::from(self.reader.read_u8().unwrap()) << offset;
            offset += 8;
        }

        let diff = bits - offset;
        if diff > 0 {
            assert!(diff < 8);
            let mask = self.mask(diff);
            self.rtmp = self.reader.read_u8().unwrap().into();
            out |= (self.rtmp & mask) << offset;
            offset += diff;
        }

        self.roffset += offset;
        out
    }

    fn write(&mut self, value: u32, mut bits: u32) {
        assert!(bits != 0 && bits <= 32);

        let mut offset = 0;
        let woffset = self.woffset % 8;
        if woffset > 0 {
            let b = bits.min(8 - woffset);
            let mask = self.mask(b) << woffset;
            *self.writer.last_mut().unwrap() |= ((value << woffset) & mask) as u8;
            offset += b;
            bits -= b;
        }

        while bits >= 8 {
            self.writer.push((value >> offset) as u8);
            offset += 8;
            bits -= 8;
        }

        if bits > 0 {
            let mask = self.mask(bits);
            self.writer.push(((value >> offset) & mask) as u8);
            offset += bits;
        }

        self.woffset += offset;
    }

    fn write_bool(&mut self, value: bool) {
        let b = if value {
            1
        } else {
            0
        };
        self.write(b, 1);
    }

    fn tee(&mut self, bits: u32) -> u32 {
        let out = self.read(bits);
        self.write(out, bits);
        out
    }

    fn copy(&mut self, mut size: usize) {
        while size > 0 {
            let bits = size.min(32) as u32;
            let b = self.read(bits);
            self.write(b, bits);
            size -= bits as usize;
        }
    }

    fn flush(&mut self) {
        let woffset = self.woffset % 8;
        if woffset != 0 {
            self.woffset += 8 - woffset;
        }
    }

    fn set_bit(&mut self, offset: u32) {
        let i = offset / 8;
        let o = offset % 8;
        self.writer[i as usize] |= 1 << o;
    }
}

fn codebook_rebuild(
    bio: &mut BitIo,
    codebook_id: u32,
) -> io::Result<()> {
    let mut codebook = AOTUV603_CODEBOOK.data(codebook_id).unwrap();
    let codebook_len = codebook.len();
    let woffset = bio.woffset;
    let mut bio2 = BitIo::new(&mut codebook, bio.writer);
    bio2.woffset = woffset;

    let id = 0x564342;
    let dimensions = bio2.read(4);
    let entries = bio2.read(14);
    bio2.write(id, 24);
    bio2.write(dimensions, 16);
    bio2.write(entries, 24);

    let ordered = bio2.tee(1);
    if ordered != 0 {
        let _initial_len = bio2.tee(5);
        let mut current_entry = 0;
        while current_entry < entries {
            let b = msb(entries - current_entry);
            current_entry += bio2.tee(b);
        }
        assert!(current_entry <= entries);
    } else {
        let codeword_len_len = bio2.read(3);
        let sparse = bio2.tee(1);
        assert!(codeword_len_len > 0 && codeword_len_len <= 5);

        for _ in 0..entries {
            let mut present = true;

            if sparse != 0 {
                present = bio2.tee(1) != 0;
            }

            if present {
                let codeword_len = bio2.read(codeword_len_len);
                bio2.write(codeword_len, 5);
            }
        }
    }

    let lookup_kind = bio2.read(1);
    bio2.write(lookup_kind, 4);
    if lookup_kind == 1 {
        let _min = bio2.tee(32);
        let _max = bio2.tee(32);
        let value_len = bio2.tee(4);
        let _sequence_flag = bio2.tee(1);

        let quantvals = {
            let bits = msb(entries);
            let mut vals = entries >> ((bits - 1) * (dimensions - 1) / dimensions);

            loop {
                let mut acc = 1;
                let mut acc1 = 1;
                for _ in 0..dimensions {
                    acc *= vals;
                    acc1 *= vals + 1;
                }

                if acc <= entries && acc1 > entries {
                    break vals;
                } else {
                    if acc > entries {
                        vals -= 1;
                    } else {
                        vals += 1;
                    }
                }
            }
        };
        for _ in 0..quantvals {
            let val_bits = value_len + 1;
            bio2.tee(val_bits);
        }
    }

    assert_eq!(bio2.roffset as usize / 8 + 1, codebook_len);
    let woffset = bio2.woffset;
    bio.woffset = woffset;

    Ok(())
}

struct OggStream {
    buf: Vec<u8>,
    granule: u64,
    first: bool,
    reserved: usize,
    patch_page: usize,
    segments_used: usize,
    page_count: u32,
}

impl OggStream {
    const DEFAULT_SEGMENT_COUNT: usize = 0x25;
    const HEADER_SIZE: usize = 27;

    fn new(buf: Vec<u8>) -> Self {
        Self {
            buf,
            granule: 0,
            first: true,
            reserved: 0,
            patch_page: 0,
            segments_used: 0,
            page_count: 0,
        }
    }

    fn start_page_(&mut self, segment_count: usize, spill: Option<usize>) {
        assert!(segment_count < 256);

        if self.page_count > 0 {
            self.update_granule();

            let buf = &mut self.buf[self.patch_page..];
            let scount = buf[26];
            let mut used = 0;
            let mut last = 0;
            for &b in &buf[27..27 + scount as usize] {
                if b == 0 && last != 255 {
                    break;
                }
                last = b;
                used += 1;
            }
            if used < scount {
                let diff = usize::from(scount - used);
                buf[26] = used;
                buf[27 + used as usize..].rotate_left(diff);
                let len = self.buf.len();
                self.buf.truncate(len - diff);
            }
        }

        let mut len = self.buf.len();
        self.buf.resize(len + Self::HEADER_SIZE + segment_count, 0);

        if let Some(spill) = spill {
            len -= spill;
            self.buf[len..len + spill + Self::HEADER_SIZE + segment_count].rotate_left(spill);
        }

        let mut flag = 0;
        if spill.is_some() {
            flag |= 0x01;
        }
        if self.first {
            flag |= 0x02;
        }

        let granule: u64 = if spill >= Some(segment_count * 255) {
            0xffffffffffffffff
        } else {
            0
        };

        let header = &mut self.buf[len..];
        header[..4].copy_from_slice(b"OggS");
        header[4] = 0;
        header[5] = flag;
        header[6..14].copy_from_slice(&granule.to_le_bytes());
        header[14..18].copy_from_slice(&0xefbeadde_u32.to_le_bytes());
        header[18..22].copy_from_slice(&self.page_count.to_le_bytes());
        header[22..26].copy_from_slice(&0_u32.to_le_bytes());
        header[26] = segment_count as u8;

        self.first = false;
        self.patch_page = len;
        self.segments_used = 0;
        self.reserved = self.buf.len();
        self.page_count += 1;
    }

    fn start_page(&mut self) {
        self.start_page_(Self::DEFAULT_SEGMENT_COUNT, None);
    }

    fn start_info_page(&mut self) {
        self.start_page_(1, None);
    }

    fn add_packet_(&mut self, size: usize) {
        let o = self.patch_page + Self::HEADER_SIZE;
        let num_segments = self.buf[o - 1] as usize;
        let segments = &mut self.buf[o..o + num_segments];
        let mut i = self.segments_used;

        assert_eq!(0, segments[i]);

        let mut len = size;
        let mut last_255 = false;
        while i < num_segments && len > 0 {
            let l = len.min(255);
            len -= l;
            last_255 = l == 255;
            segments[i] = u8::try_from(l).unwrap();
            i += 1;
        }
        if last_255 && i < num_segments {
            segments[i] = 0;
            i += 1;
            last_255 = false;
        }
        if i >= num_segments {
            assert_eq!(i, num_segments);
            if len > 0 || last_255 {
                self.start_page_(Self::DEFAULT_SEGMENT_COUNT, Some(len));
                self.add_packet_(len);
            } else {
                self.start_page_(Self::DEFAULT_SEGMENT_COUNT, None);
            }
            return;
        }
        self.segments_used = i;
        self.reserved = self.buf.len();
    }

    fn add_packet(&mut self) {
        let len = self.buf.len() - self.reserved;
        self.add_packet_(len);
    }

    fn increment_granule(&mut self, granule: u32) {
        self.granule += granule as u64;
    }

    fn update_granule(&mut self) {
        let buf = &mut self.buf[self.patch_page..];
        if buf[6..14] != u64::MAX.to_ne_bytes() {
            buf[6..14].copy_from_slice(&self.granule.to_le_bytes());
        }
    }

    fn checksum_ogg(&mut self) {
        let mut buf = &mut self.buf[..];
        for i in 0..self.page_count {
            assert_eq!(b"OggS", &buf[..4], "OggS mismatch at page {i}");

            let o = Self::HEADER_SIZE;
            let num_segments = buf[o - 1] as usize;
            let segments = &buf[o..o + num_segments];
            let mut len = o + num_segments;
            for l in segments {
                len += *l as usize;
            }

            let page;
            (page, buf) = buf.split_at_mut(len);
            let crc = checksum(page);
            page[22..26].copy_from_slice(&crc.to_le_bytes());
        }
        assert!(buf.is_empty());
    }

    fn finish(&mut self) {
        assert_ne!(0, self.patch_page);
        self.start_page_(0, None);
        self.update_granule();
        self.buf[self.patch_page + 5] |= 0x04;
        self.checksum_ogg();
    }
}

struct StreamFmtHeader {
    channels: u8,
    setup_offset: u32,
    bs0e: u8,
    bs1e: u8,
}

fn read_fmt(
    rdr: &mut dyn Read,
    out: &mut OggStream,
) -> io::Result<StreamFmtHeader> {
    let format = rdr.read_u16::<LE>().unwrap();
    assert!(format == 0xffff, "{format:04x}");
    let channels = rdr.read_u16::<LE>().unwrap();
    let sample_rate = rdr.read_u32::<LE>().unwrap();
    let _avg_bitrate = rdr.read_u32::<LE>().unwrap();
    let _block_size = rdr.read_u16::<LE>().unwrap();
    let _bits_per_sample = rdr.read_u16::<LE>().unwrap();
    let extra_size = rdr.read_u16::<LE>().unwrap();
    assert_eq!(0x30, extra_size);
    let _unused00 = rdr.read_u16::<LE>().unwrap();
    let _channel_layout = rdr.read_u32::<LE>().unwrap();
    //24

    //assert!(sample_rate == 44100 || sample_rate == 48000, "{sample_rate}");
    assert_eq!(0xffff, format);

    let _num_samples = rdr.read_u32::<LE>().unwrap();
    let mut skip = [0; 16];
    rdr.read_exact(&mut skip[..12]).unwrap();
    //40
    let setup_offset = rdr.read_u32::<LE>().unwrap();
    let _audio_offset = rdr.read_u32::<LE>().unwrap();
    rdr.read_exact(&mut skip[..16]).unwrap();
    let bs1e = rdr.read_u8().unwrap();
    let bs0e = rdr.read_u8().unwrap();
    //66

    if bs1e != 0x08 {
        return Err(io::Error::other("unsupported"));
    }
    assert_eq!(0x08, bs1e);
    assert_eq!(0x0b, bs0e);

    out.start_info_page();

    out.buf.write_u8(1).unwrap();
    out.buf.write_all(b"vorbis").unwrap();

    out.buf.write_u32::<LE>(0).unwrap();
    let channels = u8::try_from(channels).unwrap();
    out.buf.write_u8(channels).unwrap();
    out.buf.write_u32::<LE>(sample_rate).unwrap();
    out.buf.write_u32::<LE>(0).unwrap();
    out.buf.write_u32::<LE>(0).unwrap();
    out.buf.write_u32::<LE>(0).unwrap();
    out.buf.write_u8((bs0e << 4) | bs1e).unwrap();
    out.buf.write_u8(1).unwrap();

    out.add_packet();

    out.buf.write_u8(3).unwrap();
    out.buf.write_all(b"vorbis").unwrap();

    let vendor = b"limn (github.com/manshanko/limn)";
    out.buf.write_u32::<LE>(vendor.len() as u32).unwrap();
    out.buf.write_all(vendor).unwrap();
    out.buf.write_u32::<LE>(0).unwrap();
    out.buf.write_u32::<LE>(1).unwrap();

    out.add_packet();

    Ok(StreamFmtHeader {
        channels,
        setup_offset,
        bs0e,
        bs1e,
    })
}

struct StreamDataState {
    mode_bits: u32,
    mode_block_flags: [bool; 65],
    prev_block_flag: bool,
    defer_next_block_flag: Option<u32>,

    blocksizes: [u32; 2],
    prev_blocksize: u32,
}

impl StreamDataState {
    fn new(
        header: &StreamFmtHeader,
        rdr: &mut dyn Read,
        out: &mut OggStream,
    ) -> io::Result<Self> {
        let _packet_size = rdr.read_u16::<LE>().unwrap();
        assert_eq!(0, header.setup_offset % 4);
        for _ in 0..(header.setup_offset / 4) {
            _ = rdr.read_u16::<LE>().unwrap();
            _ = rdr.read_u16::<LE>().unwrap();
        }

        out.buf.write_u8(5).unwrap();
        out.buf.write_all(b"vorbis").unwrap();

        let mut bio = BitIo::new(rdr, &mut out.buf);

        let num_codebooks = bio.tee(8) + 1;
        for _ in 0..num_codebooks {
            let id = bio.read(10);
            codebook_rebuild(&mut bio, id)?;
        }

        let _time_count_less_1 = 0;
        bio.write(_time_count_less_1, 6);
        let _dummy_time = 0;
        bio.write(_dummy_time, 16);

        let num_floors = bio.tee(6) + 1;
        for _ in 0..num_floors {
            let floor_kind = 1;
            bio.write(floor_kind, 16);

            let mut max_class = 0;
            let mut class_list = [0; 32];
            let num_parts = bio.tee(5);
            for i in 0..num_parts {
                let class = bio.tee(4);
                class_list[i as usize] = class;
                max_class = max_class.max(class);
            }
            assert!(max_class <= 0x5b);

            let mut class_dim_list = [0; 17];
            for i in 0..(max_class + 1) {
                let class_dim = bio.tee(3) + 1;
                class_dim_list[i as usize] = class_dim;
                assert!(class_dim <= 0x4b + 1);

                let subclasses = bio.tee(2);
                if subclasses != 0 {
                    let masterbook = bio.tee(8);
                    assert!(masterbook < num_codebooks);
                }

                for _ in 0..(1 << subclasses) {
                    let subclass_book = bio.tee(8) as i32 - 1;
                    assert!(subclass_book < num_codebooks as i32);
                }
            }

            let _multiplier = bio.tee(2) + 1;
            let rangebits = bio.tee(4);
            for i in 0..num_parts {
                let current = class_list[i as usize];
                for _ in 0..class_dim_list[current as usize] {
                    bio.tee(rangebits);
                }
            }
        }

        let num_residues = bio.tee(6) + 1;
        for _ in 0..num_residues {
            let kind = bio.read(2);
            bio.write(kind, 16);
            assert!(kind <= 2);

            let _begin = bio.tee(24);
            let _end = bio.tee(24);
            let _part_size = bio.tee(24) + 1;
            let num_classes = bio.tee(6) + 1;
            let classbook = bio.tee(8);
            assert!(classbook < num_codebooks);

            let mut cascade = [0; 65];
            for i in 0..num_classes {
                let lobl_put = bio.tee(3);
                let bitflag = bio.tee(1);
                let high_bits = if bitflag != 0 {
                    bio.tee(5)
                } else {
                    0
                };
                cascade[i as usize] = high_bits * 8 + lobl_put;
            }

            for i in 0..num_classes {
                for j in 0..8 {
                    if cascade[i as usize] & (1 << j) != 0 {
                        let book = bio.tee(8);
                        assert!(book < num_codebooks);
                    }
                }
            }
        }

        let num_mappings = bio.tee(6) + 1;
        for _ in 0..num_mappings {
            bio.write(0, 16);

            let submaps_flag = bio.tee(1);
            let mut submaps = 1;
            if submaps_flag != 0 {
                submaps = bio.tee(4) + 1;
            }

            let square_polar_flag = bio.tee(1);
            if square_polar_flag != 0 {
                let num_steps = bio.tee(8) + 1;

                for _ in 0..num_steps {
                    let magnitude_bits = msb(header.channels as u32 - 1);
                    let angle_bits = magnitude_bits;

                    let magnitude = bio.tee(magnitude_bits);
                    let angle = bio.tee(angle_bits);

                    assert!(angle != magnitude);
                    assert!(magnitude < header.channels.into());
                    assert!(angle < header.channels.into());
                }
            }

            let mapping_reserved = bio.tee(2);
            assert_eq!(0, mapping_reserved);

            if submaps > 1 {
                for _ in 0..header.channels {
                    let mapping_mux = bio.tee(4);
                    assert!(mapping_mux < submaps);
                }
            }

            for _ in 0..submaps {
                let _time_config = bio.tee(8);
                let floor_number = bio.tee(8);
                let residue_number = bio.tee(8);

                assert!(floor_number < num_floors);
                assert!(residue_number < num_residues);
            }
        }

        let num_modes = bio.tee(6) + 1;
        let mode_bits = msb(num_modes - 1);
        let mut mode_block_flags = [false; 65];
        for i in 0..num_modes {
            let block_flag = bio.tee(1);
            mode_block_flags[i as usize] = block_flag != 0;

            let window_kind = 0;
            bio.write(window_kind, 16);
            let transform_kind = 0;
            bio.write(transform_kind, 16);

            let mapping = bio.tee(8);
            assert!(mapping < num_mappings);
        }

        let framing = 1;
        bio.write(framing, 1);
        bio.flush();

        let blocksizes = [1 << header.bs1e, 1 << header.bs0e];
        assert!(blocksizes[0] >= 64);
        assert!(blocksizes[0] <= blocksizes[1], "{:?}", blocksizes);
        assert!(blocksizes[1] <= 8192);

        out.add_packet();
        out.start_page();

        Ok(Self {
            mode_bits,
            mode_block_flags,
            prev_block_flag: false,
            defer_next_block_flag: None,

            blocksizes,
            prev_blocksize: 0,
        })
    }

    fn read_data_packet(
        &mut self,
        rdr: &mut dyn Read,
        out: &mut OggStream,
    ) -> io::Result<bool> {
        let packet_size = match rdr.read_u16::<LE>() {
            Ok(size) => size,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(err) => return Err(err),
        };
        let mut bio = BitIo::new(rdr, &mut out.buf);

        let kind = 0;
        bio.write(kind, 1);
        let mode_number = bio.tee(self.mode_bits);

        if let Some(defer) = self.defer_next_block_flag.take() {
            let next_b = if self.mode_bits >= 8 {
                mode_number & 0xff
            } else {
                bio.rtmp.into()
            };
            let i = next_b & ((1 << self.mode_bits) - 1);
            let next_block_flag = self.mode_block_flags[i as usize];
            if next_block_flag {
                bio.set_bit(defer);
            }
        }

        let remainder = bio.read(8 - u32::from(self.mode_bits));

        if self.mode_block_flags[mode_number as usize] {
            let prev_block_flag = self.prev_block_flag;
            bio.write_bool(prev_block_flag);
            let defer = bio.woffset;
            bio.write_bool(false);

            // patch next_block_flag later
            self.defer_next_block_flag = Some(defer);
        }

        let block_flag = self.mode_block_flags[mode_number as usize];
        self.prev_block_flag = block_flag;

        bio.write(remainder, (8 - self.mode_bits).into());
        bio.copy(usize::from(packet_size - 1) * 8);
        bio.flush();

        self.finish_packet(block_flag, out);

        Ok(true)
    }

    fn finish_packet(&mut self, block_flag: bool, out: &mut OggStream) {
        let blocksize = self.blocksizes[block_flag as usize];
        let granule = if self.prev_blocksize > 0 {
            (self.prev_blocksize + blocksize) / 4
        } else {
            0
        };
        self.prev_blocksize = blocksize;

        out.add_packet();
        out.increment_granule(granule);
    }
}

pub(crate) struct WwiseStreamParser;

impl Extractor for WwiseStreamParser {
    fn extract(
        &self,
        entry: &mut Entry<'_, '_>,
        file_path: &Path,
        shared: &mut Vec<u8>,
        memory_pool: &mut Vec<u8>,
        options: &ExtractOptions,
    ) -> io::Result<u64> {
        let mut shared = &mut shared[..];
        memory_pool.clear();

        let variants = entry.variants();
        assert_eq!(1, variants.len());
        let prime = &variants[0];
        let body_size = prime.body_size;
        let tail_size = prime.tail_size;
        assert_eq!(0, prime.unknown1);
        assert_eq!(1, prime.unknown2);
        assert_eq!(12, body_size);
        assert_eq!(31, tail_size);

        let mut body = [0; 12];
        let mut data_path = [0; 31];
        entry.read_exact(&mut body).unwrap();
        entry.read_exact(&mut data_path).unwrap();

        let file = file_from_data_path(shared, &options.target, &data_path).unwrap();
        let slice;
        (slice, shared) = shared.split_at_mut(0x10000);
        _ = shared;
        let mut rdr = ChunkReader::new(slice, file);

        if options.config.contains("force-wem") {
            let parent = file_path.parent().unwrap_or(Path::new("."));
            let file_name = file_path.file_stem().unwrap().to_str().unwrap();
            let out_path = path_concat(parent, &mut shared, file_name, Some("wem"));

            return options.open(out_path, |mut fd| {
                io::copy(&mut rdr, &mut fd)
            });
        }

        let header = rdr.read_u32::<LE>().unwrap();
        let _size = rdr.read_u32::<LE>().unwrap();
        let kind = rdr.read_u32::<LE>().unwrap();
        assert_eq!(STREAM_MAGIC, header);
        assert_eq!(STREAM_KIND, kind);

        let mut out = OggStream::new(core::mem::take(memory_pool));

        assert_eq!(FMT_MAGIC, rdr.read_u32::<LE>().unwrap());
        let size = rdr.read_u32::<LE>().unwrap();

        // other sizes may not be vorbis audio
        if size != 0x42 {
            return Ok(0);
        }

        let Ok(fmt_header) = read_fmt(&mut rdr, &mut out) else {
            return Ok(0);
        };

        while let Ok(kind) = rdr.read_u32::<LE>() {
            let size = rdr.read_u32::<LE>().unwrap();
            match kind {
                DATA_MAGIC => {
                    let mut rdr_data = (&mut rdr).take(u64::from(size));
                    let mut state = StreamDataState::new(
                        &fmt_header,
                        &mut rdr_data,
                        &mut out,
                    ).unwrap();
                    while state.read_data_packet(&mut rdr_data, &mut out).unwrap() {}
                    out.finish();
                    if !cfg!(debug_assertions) {
                        break;
                    }
                }

                0x6c706d73   //smpl
                | 0x4b4e554a //JUNK
                | 0x20646b61 //"akb "
                | 0x20657563 //"cue "
                | 0x5453494c //LIST
                => {
                    for _ in 0..size {
                        _ = rdr.read_u8();
                    }
                }

                _ => todo!("unknown chunk type: {:08x}", kind.to_be()),
            }
        }
        debug_assert!(rdr.read_u8().is_err());

        let parent = file_path.parent().unwrap_or(Path::new("."));
        let file_name = file_path.file_stem().unwrap().to_str().unwrap();
        let out_path = path_concat(parent, &mut shared, file_name, Some("ogg"));
        let wrote = options.open(out_path, |mut fd| {
            let mut out = &out.buf[..];
            io::copy(&mut out, &mut fd)
        }).unwrap();

        *memory_pool = out.buf;
        Ok(wrote)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn bit_io() {
        let input: u64 = 0b0110101010101010101100101100001011001010011010101110110101011001;
        let output = &mut Vec::new();

        let mut buf: &[u8] = &input.to_le_bytes();
        let mut bio = BitIo::new(&mut buf, output);
        assert_eq!(u64::from(bio.read(5)), input & 0b11111);
        assert_eq!(u64::from(bio.read(2)), (input >> 5) & 0b11);
        assert_eq!(u64::from(bio.read(3)), (input >> 7) & 0b111);
        assert_eq!(u64::from(bio.read(7)), (input >> 10) & 0b1111111);
        assert_eq!(u64::from(bio.read(1)), (input >> 17) & 0b1);
        assert_eq!(u64::from(bio.read(10)), (input >> 18) & 0b1111111111);
        assert_eq!(u64::from(bio.read(2)), (input >> 28) & 0b11);
        assert_eq!(u64::from(bio.read(2)), (input >> 30) & 0b11);
        assert_eq!(u64::from(bio.read(15)), (input >> 32) & 0b111111111111111);
        assert_eq!(u64::from(bio.read(17)), (input >> 47) & 0b11111111111111111);
    }
}
