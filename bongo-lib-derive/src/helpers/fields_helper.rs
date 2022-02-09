use proc_macro::TokenStream;
use syn::spanned::Spanned;
use syn::{Field, GenericArgument, Ident, PathArguments, Type};

pub fn extract_idents_from_named_fields(named_fields: &[Field]) -> Vec<Ident> {
    named_fields
        .iter()
        .map(|field| field.ident.to_owned().unwrap())
        .collect::<Vec<Ident>>()
}

pub fn extract_types_from_named_fields(named_fields: &[Field]) -> Vec<Type> {
    named_fields
        .iter()
        .map(|field| field.ty.to_owned())
        .collect::<Vec<Type>>()
}

pub fn extract_data_from_fields(
    all_named_fields: &[Field],
    all_named_fields_with_attr: &[Field],
) -> (Vec<Ident>, Vec<Type>) {
    if all_named_fields_with_attr.is_empty() {
        (
            extract_idents_from_named_fields(all_named_fields),
            extract_types_from_named_fields(all_named_fields),
        )
    } else {
        (
            extract_idents_from_named_fields(all_named_fields_with_attr),
            extract_types_from_named_fields(all_named_fields_with_attr),
        )
    }
}

pub fn type_as_string(ty: &Type) -> Result<String, TokenStream> {
    match ty {
        Type::Path(p) => Ok(p.path.segments.first().unwrap().ident.to_string()),
        _ => Err(syn::Error::new(ty.span(), "Could not extract type.")
            .to_compile_error()
            .into()),
    }
}

pub fn map_type_to_sql_type(ty: &Type) -> Result<String, TokenStream> {
    match match type_as_string(ty) {
        Ok(string) => string,
        Err(ts) => return Err(ts),
    }
    .as_str()
    {
        "String" => Ok("VARCHAR(255)".to_string()),
        "i64" => Ok("INT".to_string()),
        "bool" => Ok("BOOLEAN".to_string()),
        _ => Err(syn::Error::new(ty.span(), "Type not supported.")
            .to_compile_error()
            .into()),
    }
}

pub fn type_is_option(ty: &Type) -> Result<bool, TokenStream> {
    match ty {
        Type::Path(p) => Ok(matches!(
            p.path.segments.first().unwrap().ident.to_string().as_str(),
            "Option"
        )),
        _ => Err(syn::Error::new(ty.span(), "Could not extract type.")
            .to_compile_error()
            .into()),
    }
}

pub fn extract_type_of_option(ty: &Type) -> Result<Type, TokenStream> {
    match ty {
        Type::Path(p) => match p.path.segments.first().unwrap().clone().arguments {
            PathArguments::AngleBracketed(args) => match args.args.first().unwrap() {
                GenericArgument::Type(ty) => Ok(ty.clone()),
                _ => Err(
                    syn::Error::new(ty.span(), "Could not extract type from Option.")
                        .to_compile_error()
                        .into(),
                ),
            },
            _ => Err(
                syn::Error::new(ty.span(), "Could not extract type from Option.")
                    .to_compile_error()
                    .into(),
            ),
        },
        _ => Err(
            syn::Error::new(ty.span(), "Could not extract type from Option.")
                .to_compile_error()
                .into(),
        ),
    }
}
