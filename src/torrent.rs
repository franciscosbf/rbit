use std::{
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};

use bendy::decoding::{Decoder, Error, FromBencode, Object, ResultExt};
use sha1::{Digest, Sha1};
use thiserror::Error;
use url::Url;

const MAX_PIECE_SIZE: u64 = 524288;

fn acceptable_piece_size(size_in_bytes: u64) -> bool {
    (1..=MAX_PIECE_SIZE).contains(&size_in_bytes)
}

const MAX_FILE_SIZE_IN_BYTES: u64 = 4294967296;

fn acceptable_file_size(size_in_bytes: u64) -> bool {
    (1..=MAX_FILE_SIZE_IN_BYTES).contains(&size_in_bytes)
}

const MAX_MULTI_FILE_TOTAL_SIZE_IN_BYTES: u64 = MAX_FILE_SIZE_IN_BYTES * 4;

fn acceptable_multi_file_total_size(size_in_bytes: u64) -> bool {
    (1..=MAX_MULTI_FILE_TOTAL_SIZE_IN_BYTES).contains(&size_in_bytes)
}

#[derive(Error, Debug)]
pub enum TorrentError {
    #[error("invalid file")]
    InvalidFile,
    #[error("invalid value of field '{0}'")]
    InvalidFieldValue(&'static str),
}

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
                _ => (),
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
    pieces: Vec<u8>,
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
                (b"pieces", value) => pieces = value.try_into_bytes().map(Vec::from).map(Some)?,
                (b"files", value) => {
                    let mut files_info = vec![];

                    let mut list_obj = value.try_into_list()?;
                    while let Some(file_obj) = list_obj.next_object()? {
                        files_info.push(File::decode_bencode_object(file_obj)?)
                    }

                    files = files_info.into();
                }
                _ => (),
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
                _ => (),
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
pub struct HashPiece(pub [u8; 20]);

impl Deref for HashPiece {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&[u8]> for HashPiece {
    fn from(value: &[u8]) -> Self {
        HashPiece(value.try_into().unwrap())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct HashPieces {
    buf: Vec<u8>,
}

impl HashPieces {
    fn len(&self) -> usize {
        self.buf.len() / 20
    }

    pub fn get(&self, index: u64) -> Option<HashPiece> {
        let rindex = index as usize * 20;

        if (0..self.buf.len()).contains(&rindex) {
            let sha1 = &self.buf[rindex..(rindex + 20)];
            Some(sha1.into())
        } else {
            None
        }
    }
}

impl TryFrom<Vec<u8>> for HashPieces {
    type Error = TorrentError;

    fn try_from(pieces: Vec<u8>) -> Result<Self, Self::Error> {
        if !pieces.is_empty() && pieces.len() % 20 == 0 {
            Ok(HashPieces { buf: pieces })
        } else {
            Err(TorrentError::InvalidFieldValue("info.pieces"))
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InfoHash(pub [u8; 20]);

impl InfoHash {
    pub fn hash(raw_info: &[u8]) -> Self {
        Self(Sha1::digest(raw_info).into())
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
    pub first_piece: u64,
    pub first_piece_start: u64,
    pub last_piece: u64,
    pub last_piece_end: u64,
    pub path: PathBuf,
}

impl FileMeta {
    fn new(
        length: u64,
        first_piece: u64,
        first_piece_start: u64,
        last_piece: u64,
        last_piece_end: u64,
        path: PathBuf,
    ) -> Self {
        Self {
            length,
            first_piece,
            first_piece_start,
            last_piece,
            last_piece_end,
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
pub struct TorrentInner {
    pub tracker: Url,
    pub piece_length: u64,
    pub hash_pieces: HashPieces,
    pub total_length: u64,
    pub file_type: FileType,
    pub info_hash: InfoHash,
}

impl TorrentInner {
    pub fn num_pieces(&self) -> usize {
        self.hash_pieces.len()
    }
}

#[derive(Debug, Clone)]
pub struct Torrent {
    inner: Arc<TorrentInner>,
}

impl Torrent {
    fn new(
        tracker: Url,
        piece_length: u64,
        hash_pieces: HashPieces,
        total_length: u64,
        file_type: FileType,
        info_hash: InfoHash,
    ) -> Self {
        let inner = Arc::new(TorrentInner {
            tracker,
            piece_length,
            hash_pieces,
            total_length,
            file_type,
            info_hash,
        });

        Self { inner }
    }
}

impl Deref for Torrent {
    type Target = TorrentInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl TryFrom<MetaInfo> for Torrent {
    type Error = TorrentError;

    fn try_from(file: MetaInfo) -> Result<Self, Self::Error> {
        let info = file.info;

        let tracker =
            Url::parse(&file.announce).map_err(|_| TorrentError::InvalidFieldValue("announce"))?;

        if !matches!(tracker.scheme(), "https" | "http") {
            return Err(TorrentError::InvalidFieldValue("announce"));
        }

        let raw_name = info.name.strip_suffix('/').unwrap_or(&info.name);
        let name = PathBuf::from_str(raw_name)
            .map_err(|_| TorrentError::InvalidFieldValue("info.name"))?;

        if name.parent() != Some(Path::new("")) || name.has_root() {
            return Err(TorrentError::InvalidFieldValue("info.name"));
        }

        if !acceptable_piece_size(info.piece_length) {
            return Err(TorrentError::InvalidFieldValue("info.piece"));
        }

        let piece_length = info.piece_length;

        let hash_pieces = HashPieces::try_from(info.pieces)?;

        let (file_type, length) = match (info.length, info.files) {
            (Some(length), None) => {
                if !acceptable_file_size(length) {
                    return Err(TorrentError::InvalidFieldValue("info.length"));
                }

                let name = raw_name.to_string();

                (FileType::Single { name }, length)
            }
            (None, Some(files)) => {
                let dir = name;
                let mut total_length = 0;
                let files = files
                    .into_iter()
                    .map(|f| {
                        if !acceptable_file_size(f.length) {
                            return Err(TorrentError::InvalidFieldValue("info.files.length"));
                        }

                        if f.path.is_empty() {
                            return Err(TorrentError::InvalidFieldValue("info.files.path"));
                        }

                        let path = f.path.iter().collect::<PathBuf>();

                        if !path.starts_with(raw_name) {
                            return Err(TorrentError::InvalidFieldValue("info.files.path"));
                        }

                        let file_length = f.length;
                        let prev_total_length = total_length;
                        total_length += file_length;

                        if !acceptable_multi_file_total_size(total_length) {
                            return Err(TorrentError::InvalidFieldValue("info.files.length"));
                        }

                        let first_piece = prev_total_length / piece_length;
                        let last_piece = (total_length - 1) / piece_length;

                        if last_piece as usize >= hash_pieces.len() {
                            return Err(TorrentError::InvalidFieldValue("info.pieces"));
                        }

                        let first_piece_chunk = (first_piece + 1) * piece_length;
                        let real_first_piece_start = (first_piece_chunk as i64
                            - (first_piece_chunk as i64 - prev_total_length as i64))
                            as u64;

                        let last_piece_chunk = (last_piece + 1) * piece_length;
                        let real_last_piece_end = (last_piece_chunk as i64
                            - (last_piece_chunk as i64 - total_length as i64))
                            as u64
                            - 1;

                        dbg!(real_first_piece_start, first_piece);
                        dbg!(real_last_piece_end, last_piece);
                        dbg!(piece_length, total_length);

                        let relative_first_piece_start =
                            real_first_piece_start - (first_piece * piece_length);

                        let relative_last_piece_end =
                            real_last_piece_end - (last_piece * piece_length);

                        let file_meta = FileMeta::new(
                            file_length,
                            first_piece,
                            relative_first_piece_start,
                            last_piece,
                            relative_last_piece_end,
                            path,
                        );

                        Ok(file_meta)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                if files.is_empty() {
                    return Err(TorrentError::InvalidFieldValue("info.files"));
                }

                (FileType::Multi { dir, files }, total_length)
            }
            _ => return Err(TorrentError::InvalidFile),
        };

        let info_hash = InfoHash::hash(file.raw_info.as_slice());

        let torrent = Torrent::new(
            tracker,
            piece_length,
            hash_pieces,
            length,
            file_type,
            info_hash,
        );

        Ok(torrent)
    }
}

pub fn parse_torrent_file(raw: &[u8]) -> Result<Torrent, TorrentError> {
    MetaInfo::from_bencode(raw)
        .map_err(|_| TorrentError::InvalidFile)?
        .try_into()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use claims::{assert_matches, assert_none, assert_ok, assert_some_eq};
    use url::Url;

    use super::{parse_torrent_file, FileType, HashPiece, HashPieces, TorrentError};

    #[test]
    fn get_pieces_chunk_wih_valid_chunk() {
        let hash_pieces = HashPieces {
            buf: Vec::from(b"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB".as_slice()),
        };

        assert_some_eq!(hash_pieces.get(0), HashPiece(*b"AAAAAAAAAAAAAAAAAAAA"));
        assert_some_eq!(hash_pieces.get(1), HashPiece(*b"BBBBBBBBBBBBBBBBBBBB"));
    }

    #[test]
    fn get_pieces_chunk_wih_invalid_chunk() {
        let hash_pieces = HashPieces {
            buf: Vec::from(b"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB".as_slice()),
        };

        assert_none!(hash_pieces.get(2));
    }

    #[test]
    fn parse_valid_torrent_with_single_file() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        let torrent = assert_ok!(parse_torrent_file(raw));
        assert_eq!(torrent.tracker, Url::parse("https://test.com").unwrap());
        assert_eq!(torrent.piece_length, 1);
        assert_eq!(torrent.num_pieces(), 1);
        assert_eq!(torrent.total_length, 1);
        assert_eq!(torrent.hash_pieces.buf, b"BBBBBBBBBBBBBBBBBBBB".as_slice());
        match torrent.file_type {
            FileType::Single { ref name } => {
                assert_eq!(name, "test");
            }
            _ => panic!("didn't match single file"),
        }
    }

    #[test]
    fn parse_valid_torrent_with_multi_file() {
        let raw = b"d8:announce15:http://test.com4:infod5:filesld6:lengthi4e4:pathl5:tests9:test1.txteed6:lengthi4e4:pathl5:tests9:test2.txteed6:lengthi5e4:pathl5:tests9:test3.txteee4:name5:tests12:piece lengthi5e6:pieces60:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAAee";

        let torrent = assert_ok!(parse_torrent_file(raw));
        assert_eq!(torrent.tracker, Url::parse("http://test.com").unwrap());
        assert_eq!(torrent.piece_length, 5);
        assert_eq!(torrent.num_pieces(), 3);
        assert_eq!(torrent.total_length, 13);
        assert_eq!(
            torrent.hash_pieces.buf,
            b"AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAA".as_slice()
        );
        match torrent.file_type {
            FileType::Multi { ref dir, ref files } => {
                assert_eq!(dir, Path::new("tests"));
                assert_eq!(files.len(), 3);
                let test1 = &files[0];
                assert_eq!(test1.length, 4);
                assert_eq!(test1.path, Path::new("tests/test1.txt"));
                assert_eq!(test1.first_piece, 0);
                assert_eq!(test1.first_piece_start, 0);
                assert_eq!(test1.last_piece, 0);
                assert_eq!(test1.last_piece_end, 3);
                let test2 = &files[1];
                assert_eq!(test2.length, 4);
                assert_eq!(test2.path, Path::new("tests/test2.txt"));
                assert_eq!(test2.first_piece, 0);
                assert_eq!(test2.first_piece_start, 4);
                assert_eq!(test2.last_piece, 1);
                assert_eq!(test2.last_piece_end, 2);
                let test3 = &files[2];
                assert_eq!(test3.length, 5);
                assert_eq!(test3.path, Path::new("tests/test3.txt"));
                assert_eq!(test3.first_piece, 1);
                assert_eq!(test3.first_piece_start, 3);
                assert_eq!(test3.last_piece, 2);
                assert_eq!(test3.last_piece_end, 2);
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

        let torrent = assert_ok!(parse_torrent_file(raw));
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

        let torrent = assert_ok!(parse_torrent_file(raw));
        assert_eq!(*torrent.info_hash, sha1);
    }

    #[test]
    fn parse_invalid_torrent_file() {
        let raw = b"d8:announcei45e4:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse_torrent_file(raw), Err(TorrentError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_single_file_and_unaccepted_scheme() {
        let raw = b"d8:announce19:udp://test.com:69694:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("announce"))
        );
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_announce() {
        let raw = b"d8:announce15:https//test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("announce"))
        );
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_length() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi0e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.length"))
        );

        let raw = b"d8:announce16:https://test.com4:infod6:lengthi9223372036854841342e4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.length"))
        );

        let raw = b"d8:announce16:https://test.com4:infod6:lengthi-1e\
            4:name4:test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse_torrent_file(raw), Err(TorrentError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_name() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name5:/test12:piece lengthi1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.name"))
        );
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_piece() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi0e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.piece"))
        );

        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi9223372036854841342e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.piece"))
        );

        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi-1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse_torrent_file(raw), Err(TorrentError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_pieces() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test12:piece lengthi1e6:pieces14:BBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.pieces"))
        );
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_file_length() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi0e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl5:tests5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.files.length"))
        );

        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi9223372036854775e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl5:tests5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.files.length"))
        );

        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi-1e4:pathl5:tests5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse_torrent_file(raw), Err(TorrentError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_file_path() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl6:tests_5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.files.path"))
        );

        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl6:/tests5:test1eed6:lengthi-1e4:pathl5:tests5:test2eee4:name5:tests\
            12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse_torrent_file(raw), Err(TorrentError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_number_of_pieces() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi4e4:pathl\
            5:tests9:test1.txteed6:lengthi12e4:pathl5:tests9:test2.txteee4:name5:tests\
            12:piece lengthi2e6:pieces140:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
            AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAA\
            BBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAAee";

        assert_matches!(
            parse_torrent_file(raw),
            Err(TorrentError::InvalidFieldValue("info.pieces"))
        );
    }

    #[test]
    fn parse_invalid_torrent_with_single_and_multi_file() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl5:tests5:test2eee6:lengthi2e\
            4:name5:tests12:piece lengthi2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse_torrent_file(raw), Err(TorrentError::InvalidFile));
    }

    #[test]
    fn parse_invalid_torrent_without_file() {
        let raw = b"d8:announce16:https://test.com4:infod4:name5:tests12:piece lengthi2e\
            6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse_torrent_file(raw), Err(TorrentError::InvalidFile));
    }
}
