use crate::helpers::attribute_helper::{
    extract_table_string_from_attributes, get_fields_with_attribute,
};
use crate::helpers::fields_helper::{extract_data_from_fields, type_is_option};
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Field};

pub fn select_primary(input: DeriveInput) -> TokenStream {
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

    let named_fields_primary_key_attr: Vec<Field> =
        get_fields_with_attribute("PrimaryKey", &named_fields);

    if named_fields_primary_key_attr.len() != 1 {
        panic!("There must be excatly one field with the PrimaryKey attribute");
    }

    let primary_type = named_fields_primary_key_attr.first().unwrap().ty.to_owned();

    if type_is_option(&primary_type) {
        panic!("The field marked with PrimaryKey must not be an Option.")
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
