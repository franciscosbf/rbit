use std::{
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
};

use bendy::decoding::{Decoder, Error, FromBencode, Object, ResultExt};
use serde_bytes::ByteBuf;
use sha1::{Digest, Sha1};
use url::Url;

use crate::error::RbitError;

#[derive(Debug)]
struct File {
    length: u64,
    path: Vec<String>,
}

impl FromBencode for File {
    const EXPECTED_RECURSION_DEPTH: usize = 1;

    fn decode_bencode_object(object: Object) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let mut length = None;
        let mut path = None;

        let mut dict = object.try_into_dictionary()?;
        while let Some(pair) = dict.next_pair()? {
            match pair {
                (b"length", value) => {
                    let raw_length = value.try_into_integer()?;

                    length = u64::from_str(raw_length)
                        .map(Some)
                        .map_err(Error::malformed_content)?;
                }
                (b"path", value) => {
                    path = Vec::<String>::decode_bencode_object(value)
                        .context("path")?
                        .into();
                }
                (unknown_field, _) => {
                    return Err(Error::unexpected_field(String::from_utf8_lossy(
                        unknown_field,
                    )));
                }
            }
        }

        let length = length.ok_or_else(|| Error::missing_field("length"))?;
        let path = path.ok_or_else(|| Error::missing_field("path"))?;

        Ok(File { length, path })
    }
}

#[derive(Debug)]
struct Info {
    name: String,
    piece_length: u64,
    pieces: ByteBuf,
    length: Option<u64>,
    files: Option<Vec<File>>,
}

impl FromBencode for Info {
    const EXPECTED_RECURSION_DEPTH: usize = File::EXPECTED_RECURSION_DEPTH + 1;

    fn decode_bencode_object(object: Object) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let mut length = None;
        let mut name = None;
        let mut piece_length = None;
        let mut pieces = None;
        let mut files = None;

        let mut dict_obj = object.try_into_dictionary()?;
        while let Some(pair) = dict_obj.next_pair()? {
            match pair {
                (b"name", value) => {
                    name = String::decode_bencode_object(value).map(Some)?;
                }
                (b"length", value) => {
                    let length_obj = value.try_into_integer()?;

                    length = u64::from_str(length_obj)
                        .map(Some)
                        .map_err(Error::malformed_content)?;
                }
                (b"piece length", value) => {
                    let piece_length_obj = value.try_into_integer()?;
                    piece_length = u64::from_str(piece_length_obj)
                        .map(Some)
                        .map_err(Error::malformed_content)?;
                }
                (b"pieces", value) => {
                    pieces = value.try_into_bytes().map(ByteBuf::from).map(Some)?
                }
                (b"files", value) => {
                    let mut files_info = vec![];

                    let mut list_obj = value.try_into_list()?;
                    while let Some(file_obj) = list_obj.next_object()? {
                        files_info.push(File::decode_bencode_object(file_obj)?)
                    }

                    files = files_info.into();
                }
                (unknown_field, _) => {
                    return Err(Error::unexpected_field(String::from_utf8_lossy(
                        unknown_field,
                    )));
                }
            }
        }

        let name = name.ok_or_else(|| Error::missing_field("name"))?;
        let piece_length = piece_length.ok_or_else(|| Error::missing_field("piece_length"))?;
        let pieces = pieces.ok_or_else(|| Error::missing_field("pieces"))?;

        Ok(Info {
            name,
            piece_length,
            pieces,
            length,
            files,
        })
    }
}

#[derive(Debug)]
struct MetaInfo {
    announce: String,
    info: Info,
    raw_info: Vec<u8>,
}

impl FromBencode for MetaInfo {
    const EXPECTED_RECURSION_DEPTH: usize = Info::EXPECTED_RECURSION_DEPTH + 3;

    fn decode_bencode_object(object: Object) -> Result<Self, Error>
    where
        Self: Sized,
    {
        let mut announce = None;
        let mut info_pair = None;

        let mut dict_obj = object.try_into_dictionary()?;
        while let Some(pair) = dict_obj.next_pair()? {
            match pair {
                (b"announce", value) => {
                    announce = String::decode_bencode_object(value).map(Some)?;
                }
                (b"info", value) => {
                    let raw_info = value.try_into_dictionary()?.into_raw()?;

                    let mut decoder = Decoder::new(raw_info);
                    let info_obj = match decoder.next_object()? {
                        Some(obj) => obj,
                        None => return Err(Error::missing_field("info")),
                    };

                    info_pair = Info::decode_bencode_object(info_obj)
                        .map(|info| (info, raw_info.to_vec()))
                        .map(Some)?;
                }
                (unknown_field, _) => {
                    return Err(Error::unexpected_field(String::from_utf8_lossy(
                        unknown_field,
                    )));
                }
            }
        }

        let announce = announce.ok_or_else(|| Error::missing_field("announce"))?;
        let (info, raw_info) = info_pair.ok_or_else(|| Error::missing_field("info"))?;

        Ok(MetaInfo {
            announce,
            info,
            raw_info,
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Piece<'a>(&'a [u8]);

impl Deref for Piece<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Pieces {
    buf: ByteBuf,
}

impl Pieces {
    fn len(&self) -> usize {
        self.buf.len() / 20
    }

    pub fn get_sha1(&self, index: usize) -> Option<Piece> {
        let rindex = index * 20;

        if (0..self.buf.len()).contains(&rindex) {
            let sha1 = &self.buf[rindex..(rindex + 20)];
            Some(Piece(sha1))
        } else {
            None
        }
    }
}

impl TryFrom<ByteBuf> for Pieces {
    type Error = RbitError;

    fn try_from(pieces: ByteBuf) -> Result<Self, Self::Error> {
        if !pieces.is_empty() && pieces.len() % 20 == 0 {
            Ok(Pieces { buf: pieces })
        } else {
            Err(RbitError::InvalidFieldValue("info.pieces"))
        }
    }
}

#[derive(Debug)]
pub struct InfoHash([u8; 20]);

impl From<&[u8]> for InfoHash {
    fn from(raw_info: &[u8]) -> Self {
        InfoHash(Sha1::digest(raw_info).into())
    }
}

impl Deref for InfoHash {
    type Target = [u8; 20];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct FileMeta {
    pub length: u64,
    pub start: u64,
    pub end: u64,
    pub path: PathBuf,
}

impl FileMeta {
    fn new(length: u64, start: u64, end: u64, path: PathBuf) -> Self {
        Self {
            length,
            start,
            end,
            path,
        }
    }
}

#[derive(Debug)]
pub enum FileType {
    Single { name: String },
    Multi { dir: PathBuf, files: Vec<FileMeta> },
}

#[derive(Debug)]
pub struct Torrent {
    pub tracker: Url,
    pub piece: u64,
    pub pieces: Pieces,
    pub length: u64,
    pub file_type: FileType,
    pub info_hash: InfoHash,
}

impl Torrent {
    fn new(
        tracker: Url,
        piece: u64,
        pieces: Pieces,
        length: u64,
        file_type: FileType,
        info_hash: InfoHash,
    ) -> Self {
        Self {
            tracker,
            piece,
            pieces,
            length,
            file_type,
            info_hash,
        }
    }
}

impl TryFrom<MetaInfo> for Torrent {
    type Error = RbitError;

    fn try_from(file: MetaInfo) -> Result<Self, Self::Error> {
        let info = file.info;

        let tracker =
            Url::parse(&file.announce).map_err(|_| RbitError::InvalidFieldValue("announce"))?;

        if !matches!(tracker.scheme(), "https" | "http") {
            return Err(RbitError::InvalidFieldValue("announce"));
        }

        let raw_name = info.name.strip_suffix('/').unwrap_or(&info.name);
        let name =
            PathBuf::from_str(raw_name).map_err(|_| RbitError::InvalidFieldValue("info.name"))?;

        if name.parent() != Some(Path::new("")) || name.has_root() {
            return Err(RbitError::InvalidFieldValue("info.name"));
        }

        let piece = if info.piece_length > 0 {
            info.piece_length
        } else {
            return Err(RbitError::InvalidFieldValue("info.piece"));
        };

        let pieces = Pieces::try_from(info.pieces)?;

        let (file_type, length) = match (info.length, info.files) {
            (Some(length), None) => {
                let length = if length > 0 {
                    length
                } else {
                    return Err(RbitError::InvalidFieldValue("info.length"));
                };
                let name = raw_name.to_string();

                (FileType::Single { name }, length)
            }
            (None, Some(files)) => {
                let dir = name;
                let mut current = 0;
                let mut total_length = 0;
                let files = files
                    .into_iter()
                    .map(|f| {
                        let length = if f.length > 0 {
                            total_length += f.length;
                            f.length
                        } else {
                            return Err(RbitError::InvalidFieldValue("info.files.length"));
                        };

                        if f.path.is_empty() {
                            return Err(RbitError::InvalidFieldValue("info.files.path"));
                        }

                        let path = f.path.iter().collect::<PathBuf>();

                        let start = current;
                        let shift = start + (length + piece - 1) / piece;
                        let end = shift - 1;
                        if shift as usize > pieces.len() {
                            return Err(RbitError::InvalidFieldValue("info.pieces"));
                        }
                        current = shift;

                        if path.starts_with(raw_name) {
                            Ok(FileMeta::new(length, start, end, path))
                        } else {
                            Err(RbitError::InvalidFieldValue("info.files.path"))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                if files.is_empty() {
                    return Err(RbitError::InvalidFieldValue("info.files"));
                }

                (FileType::Multi { dir, files }, total_length)
            }
            _ => return Err(RbitError::InvalidFile),
        };

        let info_hash = file.raw_info.as_slice().into();

        let torrent = Torrent::new(tracker, piece, pieces, length, file_type, info_hash);

        Ok(torrent)
    }
}

pub fn parse(raw: &[u8]) -> Result<Torrent, RbitError> {
    MetaInfo::from_bencode(raw)
        .map_err(|_| RbitError::InvalidFile)?
        .try_into()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use claims::{assert_matches, assert_none, assert_ok, assert_some_eq};
    use serde_bytes::ByteBuf;
    use url::Url;

    use super::{parse, FileType, Piece, Pieces};
    use crate::error::RbitError;

    #[test]
    fn get_pieces_chunk_wih_valid_chunk() {
        let pieces = Pieces {
            buf: ByteBuf::from(b"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB".as_slice()),
        };

        assert_some_eq!(
            pieces.get_sha1(0),
            Piece(b"AAAAAAAAAAAAAAAAAAAA".as_slice())
        );
        assert_some_eq!(
            pieces.get_sha1(1),
            Piece(b"BBBBBBBBBBBBBBBBBBBB".as_slice())
        );
    }

    #[test]
    fn get_pieces_chunk_wih_invalid_chunk() {
        let pieces = Pieces {
            buf: ByteBuf::from(b"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB".as_slice()),
        };

        assert_none!(pieces.get_sha1(2));
    }

    #[test]
    fn parse_valid_torrent_with_single_file() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        let torrent = assert_ok!(parse(raw));
        assert_eq!(torrent.tracker, Url::parse("https://test.com").unwrap());
        assert_eq!(torrent.piece, 1);
        assert_eq!(
            torrent.pieces.buf,
            ByteBuf::from(b"BBBBBBBBBBBBBBBBBBBB".as_slice())
        );
        assert_eq!(torrent.length, 1);
        match torrent.file_type {
            FileType::Single { name } => {
                assert_eq!(name, "test");
            }
            _ => panic!("didn't match single file"),
        }
    }

    #[test]
    fn parse_valid_torrent_with_multi_file() {
        let raw = b"d8:announce15:http://test.com4:infod5:filesld6:lengthi6e\
            4:pathl5:tests9:test1.txteed6:lengthi7e4:pathl5:tests9:test2.txteee\
            4:name5:tests12:piece lengthi2e6:pieces140:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
            AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAA\
            BBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAAee";

        let torrent = assert_ok!(parse(raw));
        assert_eq!(torrent.tracker, Url::parse("http://test.com").unwrap());
        assert_eq!(torrent.piece, 2);
        assert_eq!(torrent.pieces.len(), 7);
        assert_eq!(torrent.length, 13);
        assert_eq!(
            torrent.pieces.buf,
            ByteBuf::from(
                b"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
                AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
                AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAA"
                    .as_slice()
            )
        );
        match torrent.file_type {
            FileType::Multi { dir, files } => {
                assert_eq!(dir, Path::new("tests"));
                assert_eq!(files.len(), 2);
                let test1 = &files[0];
                assert_eq!(test1.length, 6);
                assert_eq!(test1.path, Path::new("tests/test1.txt"));
                assert_eq!(test1.start, 0);
                assert_eq!(test1.end, 2);
                let test2 = &files[1];
                assert_eq!(test2.length, 7);
                assert_eq!(test2.path, Path::new("tests/test2.txt"));
                assert_eq!(test2.start, 3);
                assert_eq!(test2.end, 6);
            }
            _ => panic!("didn't match multi file"),
        }
    }

    #[test]
    fn confirm_info_hash_of_single_file() {
        let raw = b"d8:announce15:http://test.com4:infod6:lengthi1e4:name\
            4:test12:piece lengthi1e6:pieces20:AAAAAAAAAAAAAAAAAAAAee";
        let sha1: &[u8] = &[
            0x56, 0xED, 0xB4, 0xA8, 0xE3, 0x51, 0xA1, 0x38, 0x26, 0x0B, 0x38, 0x0E, 0x19, 0xED,
            0xA4, 0x27, 0x71, 0xE0, 0x55, 0xA9,
        ];

        let torrent = assert_ok!(parse(raw));
        assert_eq!(*torrent.info_hash, sha1);
    }

    #[test]
    fn confirm_info_hash_of_multi_file() {
        let raw = b"d8:announce15:http://test.com4:infod5:filesld6:lengthi6e\
            4:pathl5:tests9:test1.txteed6:lengthi7e4:pathl5:tests9:test2.txteee\
            4:name5:tests12:piece lengthi2e6:pieces140:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
            AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAA\
            BBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAAee";
        let sha1: &[u8] = &[
            0x07, 0x01, 0xA5, 0x71, 0x7F, 0xCD, 0xF3, 0x66, 0x97, 0x1D, 0x3A, 0x4A, 0x14, 0xB8,
            0x5C, 0xDB, 0xE9, 0xF7, 0x1E, 0x08,
        ];

        let torrent = assert_ok!(parse(raw));
        assert_eq!(*torrent.info_hash, sha1);
    }

    #[test]
    fn parse_invalid_torrent_file() {
        let raw = b"d8:announcei45e4:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_single_file_and_unaccepted_scheme() {
        let raw = b"d8:announce19:udp://test.com:69694:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFieldValue("announce")));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_announce() {
        let raw = b"d8:announce15:https//test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFieldValue("announce")));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_length() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi0e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFieldValue("info.length")));

        let raw = b"d8:announce16:https://test.com4:infod6:lengthi-1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_name() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name5:/test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFieldValue("info.name")));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_piece() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi0e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFieldValue("info.piece")));

        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi-1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_pieces() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces14:BBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFieldValue("info.pieces")));
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_file_length() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi0e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl5:tests5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse(raw),
            Err(RbitError::InvalidFieldValue("info.files.length"))
        );

        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi-1e4:pathl5:tests5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_file_path() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl6:tests_5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse(raw),
            Err(RbitError::InvalidFieldValue("info.files.path"))
        );

        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl6:/tests5:test1eed6:lengthi-1e4:pathl5:tests5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_number_of_pieces() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi4e4:pathl\
            5:tests9:test1.txteed6:lengthi12e4:pathl5:tests9:test2.txteee4:name5:tests\
            12:piece lengthi2e6:pieces140:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
            AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAA\
            BBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAAee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFieldValue("info.pieces")));
    }

    #[test]
    fn parse_invalid_torrent_with_single_and_multi_file() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl5:tests5:test2eee6:lengthi2e\
            4:name5:tests12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }

    #[test]
    fn parse_invalid_torrent_without_file() {
        let raw = b"d8:announce16:https://test.com4:infod4:name5:tests12:piece lengthi2e\
            6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }
}
