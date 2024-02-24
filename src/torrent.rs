use std::{
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
};

use serde_bytes::ByteBuf;
use url::Url;

use crate::error::RbitError;

#[derive(Debug, serde::Deserialize)]
struct File {
    length: i64,
    path: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct Info {
    name: String,
    piece: i64,
    pieces: ByteBuf,
    length: Option<i64>,
    files: Option<Vec<File>>,
}

#[derive(Debug, serde::Deserialize)]
struct MetaInfo {
    announce: String,
    info: Info,
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
            Err(RbitError::InvalidField("info.pieces"))
        }
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

impl TryFrom<MetaInfo> for Torrent {
    type Error = RbitError;

    fn try_from(file: MetaInfo) -> Result<Self, Self::Error> {
        let info = file.info;

        let tracker =
            Url::parse(&file.announce).map_err(|_| RbitError::InvalidField("announce"))?;

        if !matches!(tracker.scheme(), "https" | "http") {
            return Err(RbitError::InvalidField("announce"));
        }

        let raw_name = info.name.strip_suffix('/').unwrap_or(&info.name);
        let name = PathBuf::from_str(raw_name).map_err(|_| RbitError::InvalidField("info.name"))?;

        if name.parent() != Some(Path::new("")) || name.has_root() {
            return Err(RbitError::InvalidField("info.name"));
        }

        let piece = if info.piece > 0 {
            info.piece as u64
        } else {
            return Err(RbitError::InvalidField("info.piece"));
        };

        let pieces = Pieces::try_from(info.pieces)?;

        let file_type = match (info.length, info.files) {
            (Some(length), None) => {
                let length = if length > 0 {
                    length as u64
                } else {
                    return Err(RbitError::InvalidField("info.length"));
                };

                FileType::Single { name, length }
            }
            (None, Some(files)) => {
                let dir = name;
                let mut current = 0;
                let files = files
                    .into_iter()
                    .map(|f| {
                        let length = if f.length > 0 {
                            f.length as u64
                        } else {
                            return Err(RbitError::InvalidField("info.files.length"));
                        };

                        let path = f.path.iter().collect::<PathBuf>();

                        let start = current;
                        let shift = start + (length + piece - 1) / piece;
                        let end = shift - 1;
                        if shift as usize > pieces.len() {
                            return Err(RbitError::InvalidField("info.pieces"));
                        }
                        current = shift;

                        if path.starts_with(raw_name) {
                            Ok(FileMeta::new(length, start, end, path))
                        } else {
                            Err(RbitError::InvalidField("info.files.path"))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                FileType::Multi { dir, files }
            }
            _ => return Err(RbitError::InvalidFile),
        };

        let torrent = Torrent::new(tracker, piece, pieces, file_type);

        Ok(torrent)
    }
}

pub fn parse(raw: &[u8]) -> Result<Torrent, RbitError> {
    serde_bencode::from_bytes::<MetaInfo>(raw)
        .map_err(|_| RbitError::InvalidFile)?
        .try_into()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use claim::{assert_matches, assert_none, assert_ok, assert_some_eq};
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
            4:name4:test5:piecei1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        let torrent = assert_ok!(parse(raw));
        assert_eq!(torrent.tracker, Url::parse("https://test.com").unwrap());
        assert_eq!(torrent.piece, 1);
        assert_eq!(
            torrent.pieces.buf,
            ByteBuf::from(b"BBBBBBBBBBBBBBBBBBBB".as_slice())
        );
        match torrent.file_type {
            FileType::Single { name, length } => {
                assert_eq!(name, Path::new("test"));
                assert_eq!(length, 1);
            }
            _ => panic!("didn't match single file"),
        }
    }

    #[test]
    fn parse_valid_torrent_with_multi_file() {
        let raw = b"d8:announce15:http://test.com4:infod5:filesld6:lengthi6e\
            4:pathl5:tests9:test1.txteed6:lengthi7e4:pathl5:tests9:test2.txteee\
            4:name5:tests5:piecei2e6:pieces140:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
            AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAA\
            BBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAAee";

        let torrent = assert_ok!(parse(raw));
        assert_eq!(torrent.tracker, Url::parse("http://test.com").unwrap());
        assert_eq!(torrent.piece, 2);
        assert_eq!(torrent.pieces.len(), 7);
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
    fn parse_invalid_torrent_file() {
        let raw = b"d8:announcei45e4:infod6:lengthi1e\
            4:name4:test5:piecei1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }

    #[test]
    fn parse_torrent_with_single_file_and_unaccepted_scheme() {
        let raw = b"d8:announce19:udp://test.com:69694:infod6:lengthi1e\
            4:name4:test5:piecei1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("announce")));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_announce() {
        let raw = b"d8:announce15:https//test.com4:infod6:lengthi1e\
            4:name4:test5:piecei1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("announce")));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_length() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi0e\
            4:name4:test5:piecei1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.length")));

        let raw = b"d8:announce16:https://test.com4:infod6:lengthi-1e\
            4:name4:test5:piecei1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.length")));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_name() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name5:/test5:piecei1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.name")));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_piece() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test5:piecei0e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.piece")));

        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test5:piecei-1e6:pieces20:BBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.piece")));
    }

    #[test]
    fn parse_torrent_with_single_file_and_invalid_pieces() {
        let raw = b"d8:announce16:https://test.com4:infod6:lengthi1e\
            4:name4:test5:piecei1e6:pieces14:BBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.pieces")));
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_file_length() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi0e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl5:tests5:test2eee4:name5:tests\
            5:piecei2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse(raw),
            Err(RbitError::InvalidField("info.files.length"))
        );

        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi-1e4:pathl5:tests5:test2eee4:name5:tests\
            5:piecei2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(
            parse(raw),
            Err(RbitError::InvalidField("info.files.length"))
        );
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_file_path() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl6:tests_5:test2eee4:name5:tests\
            5:piecei2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.files.path")));

        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl6:/tests5:test1eed6:lengthi-1e4:pathl5:tests5:test2eee4:name5:tests\
            5:piecei2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.files.path")));
    }

    #[test]
    fn parse_torrent_with_multi_file_and_invalid_number_of_pieces() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi4e4:pathl\
            5:tests9:test1.txteed6:lengthi12e4:pathl5:tests9:test2.txteee4:name5:tests\
            5:piecei2e6:pieces140:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBB\
            AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAA\
            BBBBBBBBBBBBBBBBBBBBAAAAAAAAAAAAAAAAAAAAee";

        assert_matches!(parse(raw), Err(RbitError::InvalidField("info.pieces")));
    }

    #[test]
    fn parse_invalid_torrent_with_single_and_multi_file() {
        let raw = b"d8:announce16:https://test.com4:infod5:filesld6:lengthi1e\
            4:pathl5:tests5:test1eed6:lengthi1e4:pathl5:tests5:test2eee6:lengthi2e\
            4:name5:tests5:piecei2e6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }

    #[test]
    fn parse_invalid_torrent_without_file() {
        let raw = b"d8:announce16:https://test.com4:infod4:name5:tests5:piecei2e\
            6:pieces40:AAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBBBBBBBee";

        assert_matches!(parse(raw), Err(RbitError::InvalidFile));
    }
}
