use crate::helpers::attribute_helper::get_fields_with_attribute;
use crate::helpers::fields_helper::{
    extract_idents_from_named_fields, extract_types_from_named_fields,
};
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Field};

pub fn from_row(input: DeriveInput) -> TokenStream {
    let struct_ident = input.ident;
    let fields = if let syn::Data::Struct(d) = input.data {
        d
    } else {
        panic!("FromRow derive macro is only supported for structs")
    }
    .fields;

    let named_fields = if let syn::Fields::Named(f) = fields {
        f
    } else {
        panic!("FromRow derive macro is only supported for named fields")
    }
    .named
    .into_iter()
    .collect::<Vec<Field>>();

    let named_fields_persistent_attr: Vec<Field> =
        get_fields_with_attribute("Persistent", &named_fields);

    let (field_idents, field_types) = if named_fields_persistent_attr.is_empty() {
        (
            extract_idents_from_named_fields(&named_fields),
            extract_types_from_named_fields(&named_fields),
        )
    } else {
        (
            extract_idents_from_named_fields(&named_fields_persistent_attr),
            extract_types_from_named_fields(&named_fields_persistent_attr),
        )
    };

    let with_default = if named_fields_persistent_attr.is_empty()
        || named_fields.len() == named_fields_persistent_attr.len()
    {
        quote!()
    } else {
        quote!(,..Default::default())
    };

    quote!(
        impl bongo_lib::traits::FromRow for #struct_ident {
            fn from_row(mut row: bongo_lib::types::Row) -> Self {
                Self {
                     #(#field_idents: <#field_types as bongo_lib::types::FromBongoLiteral>::from_bongo_literal(row.remove(0))),*
                     #with_default
                }
            }
        }
    ).into()
}
