// TODO: docs

use crate::types::BongoError;

pub trait AsDiscBytes<D> {
    fn as_disc_bytes(&self, def: D) -> Result<Vec<u8>, BongoError>;
}

pub trait FromDiscBytes<D> {
    fn from_disc_bytes(bytes: &[u8], def: D) -> Result<Self, BongoError>
        where Self: Sized;
}