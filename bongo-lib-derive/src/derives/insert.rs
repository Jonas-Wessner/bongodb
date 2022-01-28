use crate::helpers::attribute_helper::{
    extract_table_string_from_attributes, get_fields_with_attribute,
};
use crate::helpers::fields_helper::{
    extract_data_from_fields, extract_type_of_option, type_as_string, type_is_option,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, Field};

pub fn insert(input: DeriveInput) -> TokenStream {
    let struct_ident = input.ident;
    let table_name = match extract_table_string_from_attributes(&input.attrs) {
        None => struct_ident.to_string(),
        Some(s) => s,
    };

    let fields = if let syn::Data::Struct(d) = input.data {
        d
    } else {
        panic!("Insert derive macro is only supported for structs")
    }
    .fields;

    let named_fields = if let syn::Fields::Named(f) = fields {
        f
    } else {
        panic!("Insert derive macro is only supported for named fields")
    }
    .named
    .into_iter()
    .collect::<Vec<Field>>();

    let named_fields_persistent_attr: Vec<Field> =
        get_fields_with_attribute("Persistent", &named_fields);

    let (field_idents, field_types) =
        extract_data_from_fields(&named_fields, &named_fields_persistent_attr);

    let value_string_statements: Vec<TokenStream2> = field_idents
        .iter()
        .zip(field_types.iter())
        .map(|(ident, ty)| {
            if type_is_option(ty) {
                if type_as_string(&extract_type_of_option(ty)) == "String" {
                    quote!(
                        value_string.push_str(
                            format!("'{}'",
                                match self.#ident {
                                    None => "NULL",
                                    Some(v) => format!("{}", v).as_str(),
                                }
                            )
                            .as_str(),
                        );
                        value_string.push_str(", ");
                    )
                } else {
                    quote!(
                        value_string.push_str(
                            match self.#ident {
                                    None => "NULL".to_string(),
                                    Some(v) => format!("{}", v),
                            }
                            .as_str()
                        );
                        value_string.push_str(", ");
                    )
                }
            } else if type_as_string(ty) == "String" {
                quote!(
                    value_string.push_str(format!("'{}'", self.#ident).as_str());
                    value_string.push_str(", ");
                )
            } else {
                quote!(
                    value_string.push_str(self.#ident.to_string().as_str());
                    value_string.push_str(", ");
                )
            }
        })
        .collect();

    let mut cols = String::new();

    for ident in field_idents {
        cols.push_str(format!("{}, ", ident).as_str())
    }

    let cols = &cols[0..cols.len() - 2];

    quote!(
        impl bongo_lib::traits::InsertQuery for #struct_ident {
            fn insert_query_head() -> String {
                format!(
                    "INSERT INTO {} ({}) VALUES",
                    #table_name,
                    #cols
                )
            }

            fn insert_query_values(&self) -> String {
                let mut value_string = String::new();

                #(#value_string_statements)*

                value_string[..value_string.len() - 2].to_string()
            }
        }
    )
    .into()
}
