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
