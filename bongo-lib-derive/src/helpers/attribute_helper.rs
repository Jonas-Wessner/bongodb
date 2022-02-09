use proc_macro::TokenStream;
use syn::spanned::Spanned;
use syn::Lit::Str;
use syn::{Attribute, Field, Meta, NestedMeta};

pub fn get_fields_with_attribute(attribute_name: &str, named_fields: &[Field]) -> Vec<Field> {
    named_fields
        .iter()
        .filter(|&field| {
            field.attrs.clone().into_iter().any(|predicate| {
                predicate
                    .path
                    .segments
                    .into_iter()
                    .any(|segment| segment.ident == *attribute_name)
            })
        })
        .into_iter()
        .map(|field| field.to_owned())
        .collect::<Vec<Field>>()
}

pub fn extract_table_string_from_attributes(
    attrs: &[Attribute],
) -> Result<Option<String>, TokenStream> {
    if !attrs.is_empty() {
        match match attrs[0].parse_meta() {
            Ok(meta) => meta,
            Err(err) => {
                return Err(syn::Error::new(attrs[0].tokens.span(), err.to_string())
                    .to_compile_error()
                    .into())
            }
        } {
            Meta::List(l) => match match l.nested.first() {
                Some(meta) => meta,
                None => {
                    return Err(syn::Error::new(
                        attrs[0].tokens.span(),
                        "You have to pass a name to the TableName attribute.",
                    )
                    .to_compile_error()
                    .into())
                }
            } {
                NestedMeta::Lit(Str(s)) => Ok(Some(s.value())),
                _ => Err(syn::Error::new(
                    attrs[0].tokens.span(),
                    "The name passed to the TableName attribute must be a String.",
                )
                .to_compile_error()
                .into()),
            },
            _ => Err(syn::Error::new(
                attrs[0].tokens.span(),
                "Failed to parse TableName attribute input. Example: #[TableName(\"name\")]",
            )
            .to_compile_error()
            .into()),
        }
    } else {
        Ok(None)
    }
}
