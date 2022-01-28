use crate::types::BongoError;


///
/// An extension trait that should only be implemented by the `Row` type.
///
/// A custom trait has to be defined for this because Row is a typedef whose public interface can only
/// be extended by implementing custom traits like this.
///
pub trait AsDiscBytes<D> {
    ///
    /// Converts a `Row` to its byte representation on disc.
    ///
    /// The byte representation is defined by the values of the `BongoLiteral`s in self and the column
    /// definition. The column definition influences e.g. the length of the byte sequence for Null
    /// literals which is determined by the size of the `BongoDataType` the column was defined with.
    /// The same is true for `BongoLiteral::Varchar`s which have a different byte representation based
    /// on how their size is defined in the column definition.
    ///
    fn as_disc_bytes(&self, def: D) -> Result<Vec<u8>, BongoError>;
}

///
/// An extension trait that should only be implemented by the `Row` type.
///
/// A custom trait has to be defined for this because Row is a typedef whose public interface can only
/// be extended by implementing custom traits like this.
///
pub trait FromDiscBytes<D> {
    ///
    /// Creates a `Row` from its byte representation on disc.
    ///
    /// The byte representation is defined by the values of the `BongoLiteral`s in self and the column
    /// definition. The column definition influences e.g. the length of the byte sequence for Null
    /// literals which is determined by the size of the `BongoDataType` the column was defined with.
    /// The same is true for `BongoLiteral::Varchar`s which have a different byte representation based
    /// on how their size is defined in the column definition.
    ///
    fn from_disc_bytes(bytes: &[u8], def: D) -> Result<Self, BongoError>
        where Self: Sized;
}