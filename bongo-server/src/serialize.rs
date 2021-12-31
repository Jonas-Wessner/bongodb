///
/// `Serialize` is a trait that marks that an implementor can be serialized to a string representation
/// in the format used in the communication of `BongoServer` and the client.
///
pub trait Serialize {
    fn serialize(&self) -> String;
}