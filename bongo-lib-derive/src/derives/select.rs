use crate::helpers::attribute_helper::{
    extract_table_string_from_attributes, get_fields_with_attribute,
};
use crate::helpers::fields_helper::extract_data_from_fields;
use proc_macro::TokenStream;
use quote::quote;
use syn::Data::{Enum, Struct, Union};
use syn::Fields::{Named, Unit, Unnamed};
use syn::{DeriveInput, Field};

pub fn select(input: DeriveInput) -> TokenStream {
    let struct_ident = input.ident;
    let table_name = match match extract_table_string_from_attributes(&input.attrs) {
        Ok(opt) => opt,
        Err(ts) => return ts,
    } {
        None => struct_ident.to_string(),
        Some(s) => s,
    };

    let fields = match input.data {
        Struct(d) => d,
        Enum(d) => {
            return syn::Error::new(
                d.enum_token.span,
                "FromRow derive macro is only supported for structs",
            )
            .to_compile_error()
            .into();
        }
        Union(d) => {
            return syn::Error::new(
                d.union_token.span,
                "FromRow derive macro is only supported for structs",
            )
            .to_compile_error()
            .into();
        }
    }
    .fields;

    let named_fields = match fields {
        Named(f) => f,
        Unnamed(fields) => {
            return syn::Error::new(
                fields.paren_token.span,
                "FromRow derive macro is only supported for structs",
            )
            .to_compile_error()
            .into();
        }
        Unit => {
            return quote! {
                compile_error!("FromRow derive macro is only supported for named fields");
            }
            .into();
        }
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
