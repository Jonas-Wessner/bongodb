use crate::helpers::attribute_helper::{
    extract_table_string_from_attributes, get_fields_with_attribute,
};
use crate::helpers::fields_helper::extract_data_from_fields;
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Field};

pub fn select(input: DeriveInput) -> TokenStream {
    let struct_ident = input.ident;
    let table_name = match extract_table_string_from_attributes(&input.attrs) {
        None => struct_ident.to_string(),
        Some(s) => s,
    };

    let fields = if let syn::Data::Struct(d) = input.data {
        d
    } else {
        panic!("Select derive macro is only supported for structs")
    }
    .fields;

    let named_fields = if let syn::Fields::Named(f) = fields {
        f
    } else {
        panic!("Select derive macro is only supported for named fields")
    }
    .named
    .into_iter()
    .collect::<Vec<Field>>();

    let named_fields_persistent_attr: Vec<Field> =
        get_fields_with_attribute("Persistent", &named_fields);

    let (field_idents, _) = extract_data_from_fields(&named_fields, &named_fields_persistent_attr);

    let mut cols = String::new();
    for ident in field_idents.iter() {
        cols.push_str(format!("{}, ", ident).as_str())
    }

    let cols = &cols[0..cols.len() - 2];

    quote!(
        impl bongo_lib::traits::SelectQuery for #struct_ident {
            fn select_all_query() -> String {
                format!("SELECT {} FROM {};", #cols, #table_name)
            }

            fn select_where_query(where_clause: &str) -> String {
                format!("SELECT {} FROM {} WHERE {};", #cols, #table_name, where_clause)
            }
        }
    )
    .into()
}
