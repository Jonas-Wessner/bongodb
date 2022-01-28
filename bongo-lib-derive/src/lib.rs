extern crate core;

use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod derives;
mod helpers;

#[proc_macro_derive(CreateDropTable, attributes(TableName, Persistent))]
pub fn derive_create_drop_table(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);
    derives::create_drop_table::create_drop_table(derive_input)
}

#[proc_macro_derive(FromRow, attributes(Persistent))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);
    derives::from_row::from_row(derive_input)
}

#[proc_macro_derive(Insert, attributes(TableName, Persistent))]
pub fn derive_insert(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);
    derives::insert::insert(derive_input)
}

#[proc_macro_derive(Select, attributes(TableName, Persistent))]
pub fn derive_select(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);
    derives::select::select(derive_input)
}

#[proc_macro_derive(SelectPrimary, attributes(TableName, Persistent, PrimaryKey))]
pub fn derive_select_primary(input: TokenStream) -> TokenStream {
    let derive_input = parse_macro_input!(input as DeriveInput);
    derives::select_primary::select_primary(derive_input)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
