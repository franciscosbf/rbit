use std::ops::Deref;

pub struct PeerId(pub [u8; 20]);

impl PeerId {
    pub fn build() -> Self {
        let mut identifier = *b"-RB0100-            ";

        let arbitrary = rand::random::<[u8; 12]>();
        identifier[8..].clone_from_slice(&arbitrary);

        Self(identifier)
    }

    pub fn id(&self) -> &[u8] {
        &self.0
    }
}

impl Deref for PeerId {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

fn bytes_to_u32(raw: &[u8]) -> u32 {
    u32::from_be_bytes(raw.try_into().unwrap())
}

fn u32_to_bytes(value: u32) -> [u8; 4] {
    value.to_be_bytes()
}

enum Message {
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotIntersted,
    Have {
        piece: u32,
    },
    Bitfield {
        pieces: Vec<u8>,
    },
    Request {
        index: u32,
        begin: u32,
        length: u32,
    },
    Piece {
        index: u32,
        begin: u32,
        block: Vec<u8>,
    },
    Cancel {
        index: u32,
        begin: u32,
        length: u32,
    },
}

use Message::*;

impl Message {
    fn encode(self, buff: &mut Vec<u8>) {
        match self {
            KeepAlive => buff.extend_from_slice(&[0, 0, 0, 0]),
            Choke => buff.extend_from_slice(&[0, 0, 0, 1, 0]),
            Unchoke => buff.extend_from_slice(&[0, 0, 0, 1, 1]),
            Interested => buff.extend_from_slice(&[0, 0, 0, 1, 2]),
            NotIntersted => buff.extend_from_slice(&[0, 0, 0, 1, 3]),
            Have { piece } => {
                buff.extend_from_slice(&[0, 0, 0, 5, 4]);
                buff.extend_from_slice(&u32_to_bytes(piece));
            }
            Bitfield { pieces } => {
                buff.extend_from_slice(&u32_to_bytes(1 + pieces.len() as u32));
                buff.push(5);
                buff.extend_from_slice(&pieces);
            }
            Request {
                index,
                begin,
                length,
            } => {
                buff.extend_from_slice(&[0, 0, 0, 13, 6]);
                buff.extend_from_slice(&u32_to_bytes(index));
                buff.extend_from_slice(&u32_to_bytes(begin));
                buff.extend_from_slice(&u32_to_bytes(length));
            }
            Piece {
                index,
                begin,
                block,
            } => {
                buff.extend_from_slice(&u32_to_bytes(9 + block.len() as u32));
                buff.push(7);
                buff.extend_from_slice(&u32_to_bytes(index));
                buff.extend_from_slice(&u32_to_bytes(begin));
                buff.extend_from_slice(&block);
            }
            Cancel {
                index,
                begin,
                length,
            } => {
                buff.extend_from_slice(&[0, 0, 0, 13, 8]);
                buff.extend_from_slice(&u32_to_bytes(index));
                buff.extend_from_slice(&u32_to_bytes(begin));
                buff.extend_from_slice(&u32_to_bytes(length));
            }
        }
    }

    fn decode(content: &[u8]) -> Option<Self> {
        if content.is_empty() {
            return Some(KeepAlive);
        }

        let id = content[0];
        let content = &content[1..];

        match id {
            0 => Choke,
            1 => Unchoke,
            2 => Interested,
            3 => NotIntersted,
            4 => {
                let piece = bytes_to_u32(content);

                Have { piece }
            }
            5 => {
                let pieces = content[1..].into();

                Bitfield { pieces }
            }
            6 => {
                let index = bytes_to_u32(&content[0..4]);
                let begin = bytes_to_u32(&content[4..8]);
                let length = bytes_to_u32(&content[8..12]);

                Request {
                    index,
                    begin,
                    length,
                }
            }
            7 => {
                let index = bytes_to_u32(&content[0..4]);
                let begin = bytes_to_u32(&content[4..8]);
                let block = content[8..].into();

                Piece {
                    index,
                    begin,
                    block,
                }
            }
            8 => {
                let index = bytes_to_u32(&content[0..4]);
                let begin = bytes_to_u32(&content[4..8]);
                let length = bytes_to_u32(&content[8..12]);

                Cancel {
                    index,
                    begin,
                    length,
                }
            }
            _ => return None,
        }
        .into()
    }
}
