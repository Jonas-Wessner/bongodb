use crate::helpers::attribute_helper::{
    extract_table_string_from_attributes, get_fields_with_attribute,
};
use crate::helpers::fields_helper::{
    extract_data_from_fields, extract_type_of_option, map_type_to_sql_type, type_is_option,
};
use proc_macro::TokenStream;
use quote::quote;
use syn::Data::{Enum, Struct, Union};
use syn::Fields::{Named, Unit, Unnamed};
use syn::{DeriveInput, Field};

pub fn create_drop_table(input: DeriveInput) -> TokenStream {
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

    let (field_idents, field_types) =
        extract_data_from_fields(&named_fields, &named_fields_persistent_attr);

    let mut cols = String::new();

    for (ident, ty) in field_idents.iter().zip(field_types.iter()) {
        if match type_is_option(ty) {
            Ok(b) => b,
            Err(ts) => return ts,
        } {
            let ty = match extract_type_of_option(ty) {
                Ok(ty) => ty,
                Err(ts) => return ts,
            }
            .clone();
            cols.push_str(
                format!(
                    "{} {}, ",
                    ident,
                    match map_type_to_sql_type(&ty) {
                        Ok(string) => string,
                        Err(ts) => return ts,
                    }
                )
                .as_str(),
            )
        } else {
            cols.push_str(
                format!(
                    "{} {}, ",
                    ident,
                    match map_type_to_sql_type(ty) {
                        Ok(string) => string,
                        Err(ts) => return ts,
                    }
                )
                .as_str(),
            )
        }
    }

    let cols = cols[0..cols.len() - 2].to_string();

    quote!(
        impl bongo_lib::traits::CreateDropTableQuery for #struct_ident {
            fn create_table_query() -> String {
                format!("CREATE TABLE {} ({});", #table_name, #cols)
            }

            fn drop_table_query() -> String {
                format!("DROP TABLE {};", #table_name)
            }
        }
    )
    .into()
}
