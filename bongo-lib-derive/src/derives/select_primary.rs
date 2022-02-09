use crate::helpers::attribute_helper::{
    extract_table_string_from_attributes, get_fields_with_attribute,
};
use crate::helpers::fields_helper::{extract_data_from_fields, type_is_option};
use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::Data::{Enum, Struct, Union};
use syn::Fields::{Named, Unit, Unnamed};
use syn::{DeriveInput, Field};

pub fn select_primary(input: DeriveInput) -> TokenStream {
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

    let named_fields_primary_key_attr: Vec<Field> =
        get_fields_with_attribute("PrimaryKey", &named_fields);

    if named_fields_primary_key_attr.len() != 1 {
        return quote! {
            compile_error!("There must be excatly one field with the PrimaryKey attribute");
        }
        .into();
    }

    let primary_type = named_fields_primary_key_attr.first().unwrap().ty.to_owned();

    if match type_is_option(&primary_type) {
        Ok(b) => b,
        Err(ts) => return ts,
    } {
        return syn::Error::new(
            primary_type.span(),
            "FromRow derive macro is only supported for structs",
        )
        .to_compile_error()
        .into();
    }

    let (field_idents, _) = extract_data_from_fields(&named_fields, &named_fields_persistent_attr);

    let mut cols = String::new();
    for ident in field_idents.iter() {
        cols.push_str(format!("{}, ", ident).as_str())
    }

    let primary_ident_string = named_fields_primary_key_attr[0]
        .ident
        .to_owned()
        .unwrap()
        .to_string();

    let cols = &cols[0..cols.len() - 2];

    quote!(
        impl bongo_lib::traits::SelectPrimaryQuery<#primary_type> for #struct_ident {
            fn select_primary_query(primary: #primary_type) -> String {
                format!("SELECT {} FROM {} WHERE {}={};", #cols, #table_name, #primary_ident_string, primary)
            }
        }
    )
    .into()
}
