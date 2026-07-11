limn
=====

limn is an extractor for the bundle format used in the game Warhammer 40k Darktide.

### Linux Support

limn requires Wine on Linux since it loads the compression library used by Darktide.

## Examples

Extract all files:
```
limn -i "C:\Program Files (x86)\Steam\steamapps\common\Warhammer 40,000 Darktide\bundle" *
```

Extract only lua files:
```
limn -i "C:\Program Files (x86)\Steam\steamapps\common\Warhammer 40,000 Darktide\bundle" lua
```

With the Steam version of Darktide automatic directory lookup is used when no path is specified:
```
limn lua
```

See `limn --help` for more options.

## Dictionary

By default limn reads the file `dictionary.txt` for reverse hash lookup.

To use a specific dictionary:
```
limn --dict dictionary_hashcat_dt.txt *
```

[qasikfwn](https://github.com/qasikfwn) ([GitLab](https://gitlab.com/qasikfwn)) has a high quality dictionary as part of [Bitsquid Blender Tools](https://gitlab.com/qasikfwn/bitsquid-blender-tools).
[Download `dictionary_hashcat_dt.txt`](https://gitlab.com/qasikfwn/bitsquid-blender-tools/-/raw/dev/bitsquid/murmur/dictionaries/dictionary_hashcat_dt.txt) from the `dev` branch.

Note: when using a dictionary limn will currently only extract files with known names.

## Supported File Types

limn only supports a few file types used in Darktide bundles.

### lua

Fatshark uses a private fork of LuaJIT in Darktide. All `lua` files are stored as LuaJIT bytecode that, aside from a header version change, is compatible with existing tooling for LuaJIT (like any decompilers).

### package

`package` files in Darktide are a list of other files with a extension hash and name hash per entry.

For example most `unit` files share the same name as their `package` file which can then be referenced for the hashes of `texture` or other files used by that `unit`.

### strings

If filtering for only `strings` files then limn will either:
1. extract strings with known keys if a dictionary is used
2. extract all strings

### texture

`texture` files are stored as DDS. For mipmap levels 64KiB or larger Darktide deduplicates them to a resource file at `data/**/*`.

limn will export the highest quality mipmap level found.

For converting DDS to PNG [texconv](https://github.com/Microsoft/DirectXTex/wiki/Texconv) and [ffmpeg](https://ffmpeg.org/) can be used:
```bash
texconv -ft bmp -f B8G8R8A8_UNORM -y texture_file.dds
ffmpeg -i texture_file.BMP texture_file.png
```

### wwise_stream

Converts Wwise Vorbis streams to Ogg Vorbis.
Other types of streams are sliently ignored.

To extract the original Wwise streams as `wem` files use `--config force-wem`:
```
limn --config force-wem wwise_stream
```
