use crate::helpers::attribute_helper::get_fields_with_attribute;
use crate::helpers::fields_helper::{
    extract_idents_from_named_fields, extract_types_from_named_fields,
};
use proc_macro::TokenStream;
use quote::quote;
use syn::Data::{Enum, Struct, Union};
use syn::Fields::{Named, Unit, Unnamed};
use syn::{DeriveInput, Field};

pub fn from_row(input: DeriveInput) -> TokenStream {
    let struct_ident = input.ident;

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
        impl bongo_lib::traits::FromRow<#struct_ident> for #struct_ident {
            fn from_row(mut row: bongo_lib::types::Row) -> Result<#struct_ident, bongo_lib::types::BongoError> {
                Ok(Self {
                     #(#field_idents: <#field_types as std::convert::TryFrom<bongo_lib::types::BongoLiteral>>::try_from(row.remove(0))?),*
                     #with_default
                })
            }
        }
    ).into()
}
