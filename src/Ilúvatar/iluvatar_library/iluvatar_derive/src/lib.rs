use proc_macro::{self, TokenStream};
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(ToAny)]
pub fn derive(input: TokenStream) -> TokenStream {
    let DeriveInput { ident, .. } = parse_macro_input!(input);
    let output = quote! {
        impl iluvatar_library::types::ToAny for #ident {
            #[inline(always)]
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }
    };
    output.into()
}
