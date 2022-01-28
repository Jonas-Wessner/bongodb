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

pub fn extract_table_string_from_attributes(attrs: &[Attribute]) -> Option<String> {
    if !attrs.is_empty() {
        match attrs[0].parse_meta().unwrap_or_else(|_| {
            panic!("Could not parse attribute TableName to meta.");
        }) {
            Meta::List(l) => match l.nested.first().unwrap_or_else(|| {
                panic!("You have to pass a name to the TableName attribute.");
            }) {
                NestedMeta::Lit(Str(s)) => Some(s.value()),
                _ => {
                    panic!("The name passed to the TableName attribute must be a String.");
                }
            },
            _ => {
                panic!(
                    "Failed to parse TableName attribute input. Example: #[TableName(\"name\")]"
                );
            }
        }
    } else {
        None
    }
}
