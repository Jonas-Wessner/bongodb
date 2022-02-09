use crate::helpers::attribute_helper::{
    extract_table_string_from_attributes, get_fields_with_attribute,
};
use crate::helpers::fields_helper::{
    extract_data_from_fields, extract_type_of_option, type_as_string, type_is_option,
};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::Data::{Enum, Struct, Union};
use syn::Fields::{Named, Unit, Unnamed};
use syn::{DeriveInput, Field};

pub fn insert(input: DeriveInput) -> TokenStream {
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

    let value_string_statements: Vec<TokenStream2> = match field_idents
        .iter()
        .zip(field_types.iter())
        .map(|(ident, ty)| {
            if match type_is_option(ty) {
                Ok(b) => b,
                Err(ts) => return Err(ts),
            } {
                if match type_as_string(&match extract_type_of_option(ty) {
                    Ok(ty) => ty,
                    Err(ts) => return Err(ts),
                }) {
                    Ok(string) => string,
                    Err(ts) => return Err(ts),
                } == "String"
                {
                    Ok(quote!(
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
                    ))
                } else {
                    Ok(quote!(
                        value_string.push_str(
                            match self.#ident {
                                    None => "NULL".to_string(),
                                    Some(v) => format!("{}", v),
                            }
                            .as_str()
                        );
                        value_string.push_str(", ");
                    ))
                }
            } else if match type_as_string(ty) {
                Ok(string) => string,
                Err(ts) => return Err(ts),
            } == "String"
            {
                Ok(quote!(
                    value_string.push_str(format!("'{}'", self.#ident).as_str());
                    value_string.push_str(", ");
                ))
            } else {
                Ok(quote!(
                    value_string.push_str(self.#ident.to_string().as_str());
                    value_string.push_str(", ");
                ))
            }
        })
        .collect::<Result<Vec<TokenStream2>, TokenStream>>()
    {
        Ok(vec) => vec,
        Err(ts) => return ts,
    };

    let mut cols = String::new();

    for ident in field_idents {
        cols.push_str(format!("{}, ", ident).as_str())
    }

    let cols = &cols[0..cols.len() - 2];

    quote!(
        impl bongo_lib::traits::InsertQuery for &#struct_ident {
            fn insert_query_head() -> String {
                format!(
                    "INSERT INTO {} ({}) VALUES",
                    #table_name,
                    #cols
                )
            }

            fn insert_query_values(&self) -> String {
                let mut value_string = "(".to_string();

                #(#value_string_statements)*

                value_string = value_string[..value_string.len() - 2].to_string();

                value_string.push(')');
                value_string
            }
        }

        impl bongo_lib::traits::InsertQuery for #struct_ident {
            fn insert_query_head() -> String {
                format!(
                    "INSERT INTO {} ({}) VALUES",
                    #table_name,
                    #cols
                )
            }

            fn insert_query_values(&self) -> String {
                let mut value_string = "(".to_string();

                #(#value_string_statements)*

                value_string = value_string[..value_string.len() - 2].to_string();

                value_string.push(')');
                value_string
            }
        }
    )
    .into()
}
