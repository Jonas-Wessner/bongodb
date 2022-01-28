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

pub fn type_as_string(ty: &Type) -> String {
    match ty {
        Type::Path(p) => p.path.segments.first().unwrap().ident.to_string(),
        _ => {
            panic!("Could not extract type");
        }
    }
}

pub fn map_type_to_sql_type(ty: &Type) -> String {
    match type_as_string(ty).as_str() {
        "String" => "VARCHAR(255)".to_string(),
        "i64" => "INT".to_string(),
        "bool" => "BOOLEAN".to_string(),
        _ => {
            panic!("Type not supported")
        }
    }
}

pub fn type_is_option(ty: &Type) -> bool {
    match ty {
        Type::Path(p) => matches!(
            p.path.segments.first().unwrap().ident.to_string().as_str(),
            "Option"
        ),
        _ => {
            panic!("Could not extract type");
        }
    }
}

pub fn extract_type_of_option(ty: &Type) -> Type {
    match ty {
        Type::Path(p) => match p.path.segments.first().unwrap().clone().arguments {
            PathArguments::AngleBracketed(args) => match args.args.first().unwrap() {
                GenericArgument::Type(ty) => ty.clone(),
                _ => {
                    panic!("Could not extract type from Option.");
                }
            },
            _ => {
                panic!("Could not extract type from Option.");
            }
        },
        _ => {
            panic!("Could not extract type from Option.");
        }
    }
}
