use crate::helpers::attribute_helper::{
    extract_table_string_from_attributes, get_fields_with_attribute,
};
use crate::helpers::fields_helper::{
    extract_data_from_fields, extract_type_of_option, map_type_to_sql_type, type_is_option,
};
use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Field};

pub fn create_drop_table(input: DeriveInput) -> TokenStream {
    let struct_ident = input.ident;
    let table_name = match extract_table_string_from_attributes(&input.attrs) {
        None => struct_ident.to_string(),
        Some(s) => s,
    };

    let fields = if let syn::Data::Struct(d) = input.data {
        d
    } else {
        panic!("CreateDropTable derive macro is only supported for structs")
    }
    .fields;

    let named_fields = if let syn::Fields::Named(f) = fields {
        f
    } else {
        panic!("CreateDropTable derive macro is only supported for named fields")
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
        if type_is_option(ty) {
            let ty = extract_type_of_option(ty).clone();
            cols.push_str(format!("{} {}, ", ident, map_type_to_sql_type(&ty)).as_str())
        } else {
            cols.push_str(format!("{} {}, ", ident, map_type_to_sql_type(ty)).as_str())
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
