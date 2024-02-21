use std::{
    path::{Path, PathBuf},
    str::FromStr,
};

use serde_bytes::ByteBuf;
use url::Url;

use crate::error::RbitError;

#[derive(Debug)]
struct NonZeroUnsigned(u64);

impl From<u64> for NonZeroUnsigned {
    fn from(value: u64) -> Self {
        NonZeroUnsigned(value)
    }
}

impl<'de> serde::Deserialize<'de> for NonZeroUnsigned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct NonZeroUnsignedVisitor;

        impl<'de> serde::de::Visitor<'de> for NonZeroUnsignedVisitor {
            type Value = NonZeroUnsigned;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("positive integer")
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if v > 0 {
                    Ok(NonZeroUnsigned(v as u64))
                } else {
                    Err(E::custom("negative/zero value isn't allowed"))
                }
            }
        }

        deserializer.deserialize_i64(NonZeroUnsignedVisitor)
    }
}

fn deserialize_url<'de, D>(deserializer: D) -> Result<Url, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct UrlVisitor;

    impl<'de> serde::de::Visitor<'de> for UrlVisitor {
        type Value = Url;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let url = Url::parse(v).map_err(|_| E::custom("invalid url"))?;

            Ok(url)
        }
    }

    deserializer.deserialize_str(UrlVisitor)
}

fn deserialize_single_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct SinglePathVisitor;

    impl<'de> serde::de::Visitor<'de> for SinglePathVisitor {
        type Value = PathBuf;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            let path = PathBuf::from_str(v.strip_suffix('/').unwrap_or(v))
                .map_err(|_| E::custom("invalid path"))?;

            if path.parent() != Some(Path::new("")) {
                return Err(E::custom("path isn't unique"));
            }

            if path.has_root() {
                return Err(E::custom("path contains root"));
            }

            Ok(path)
        }
    }

    deserializer.deserialize_str(SinglePathVisitor)
}

fn deserialize_multi_path<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct MultiPathVisitor;

    impl<'de> serde::de::Visitor<'de> for MultiPathVisitor {
        type Value = PathBuf;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("sequency of strings")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut raw = vec![];
            while let Some(item) = seq.next_element::<&str>()? {
                raw.push(item);
            }

            if raw.is_empty() {
                return Err(serde::de::Error::custom("invalid path"));
            }

            let path = raw.iter().collect::<PathBuf>();

            if path.has_root() {
                return Err(serde::de::Error::custom("invalid path"));
            }

            Ok(path)
        }
    }

    deserializer.deserialize_seq(MultiPathVisitor)
}

fn deserialize_byte_buf<'de, D>(deserializer: D) -> Result<ByteBuf, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    match ByteBuf::deserialize(deserializer) {
        Ok(buf) => {
            if !buf.is_empty() && buf.len() % 20 == 0 {
                Ok(buf)
            } else {
                Err(serde::de::Error::custom("invalid byte sequence"))
            }
        }
        e => e,
    }
}

#[derive(Debug, serde::Deserialize)]
struct File {
    length: NonZeroUnsigned,
    #[serde(deserialize_with = "deserialize_multi_path")]
    path: PathBuf,
}

#[derive(Debug, serde::Deserialize)]
struct Info {
    #[serde(deserialize_with = "deserialize_single_path")]
    name: PathBuf,
    piece: NonZeroUnsigned,
    #[serde(deserialize_with = "deserialize_byte_buf")]
    pieces: ByteBuf,
    length: Option<NonZeroUnsigned>,
    files: Option<Vec<File>>,
}

#[derive(Debug, serde::Deserialize)]
struct Metainfo {
    #[serde(deserialize_with = "deserialize_url")]
    announce: Url,
    info: Info,
}

#[derive(Debug)]
pub struct Pieces {
    pieces: ByteBuf,
}

impl Pieces {
    fn new(pieces: ByteBuf) -> Self {
        Self { pieces }
    }

    pub fn get_sha1(&self, index: usize) -> Option<&[u8]> {
        let rindex = index * 20;

        if !(0..self.pieces.len()).contains(&rindex) {
            return None;
        }

        Some(&self.pieces[rindex..(rindex + 20)])
    }
}

#[derive(Debug)]
pub struct FileMeta {
    pub length: u64,
    pub path: PathBuf,
}

impl FileMeta {
    fn new(length: u64, path: PathBuf) -> Self {
        Self { length, path }
    }
}

#[derive(Debug)]
pub enum FileType {
    Single { name: PathBuf, length: u64 },
    Multi { dir: PathBuf, files: Vec<FileMeta> },
}

#[derive(Debug)]
pub struct Torrent {
    pub tracker: Url,
    pub piece: u64,
    pub pieces: Pieces,
    pub file_type: FileType,
}

impl Torrent {
    fn new(tracker: Url, piece: u64, pieces: Pieces, file_type: FileType) -> Self {
        Self {
            tracker,
            piece,
            pieces,
            file_type,
        }
    }
}

pub fn parse(raw: &[u8]) -> Result<Torrent, RbitError> {
    let file = serde_bencode::from_bytes::<Metainfo>(raw).map_err(|_| RbitError::ParseFailed)?;

    let info = file.info;

    let file_type = match (info.length, info.files) {
        (Some(length), None) => {
            let name = info.name;
            let length = length.0;

            FileType::Single { name, length }
        }
        (None, Some(files)) => {
            let dir = info.name;
            let files = files
                .into_iter()
                .map(|f| {
                    let length = f.length.0;
                    let path = f.path;
                    if path.starts_with(dir.to_str().unwrap()) {
                        Err(RbitError::ParseFailed)
                    } else {
                        Ok(FileMeta::new(length, path))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;

            FileType::Multi { dir, files }
        }
        _ => return Err(RbitError::ParseFailed),
    };

    let torrent = Torrent::new(
        file.announce,
        info.piece.0,
        Pieces::new(info.pieces),
        file_type,
    );

    Ok(torrent)
}

#[cfg(test)]
mod tests {
    use claim::assert_ok;
    use serde_bytes::ByteBuf;
    use url::Url;

    use crate::parse;

    use super::{Info, Metainfo, NonZeroUnsigned};

    #[test]
    fn parse_valid_torrent() {
        let raw = b"d8:announce17:https://test.com/4:infod6:lengthi32e4:name4:test5:piecei1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        let torrent = assert_ok!(parse(raw));
    }
}
