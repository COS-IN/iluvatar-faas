use proc_macro::{self, TokenStream};
use quote::{quote, ToTokens};
use syn::parse::Parser;
use syn::{parse_macro_input, parse_quote, DeriveInput, Lit};

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

fn format_tokens(input: syn::ItemFn, sim: bool, workers: usize) -> TokenStream {
    let syn::ItemFn {
        mut attrs,
        vis,
        mut sig,
        block,
    } = input;
    attrs.push(parse_quote! { #[::core::prelude::v1::test] });
    sig.asyncness = None;
    let stmts = block.stmts;
    let rt = match sim {
        true => quote! {
            tokio::runtime::Builder::new_current_thread()
            .start_paused(true)// Simulated runtime will always be paused
        },
        false => quote! {
            tokio::runtime::Builder::new_multi_thread()
        },
    };
    let scope = match sim {
        true => quote! {
            iluvatar_library::sync_sim_scope!
        },
        false => quote! {
            iluvatar_library::sync_live_scope!
        },
    };
    let workers = quote! { #workers };
    quote! {
        #(#attrs)* #vis #sig {
            #scope(|| {
                let rt = #rt
                    .worker_threads(#workers)
                    .enable_all()
                    .build()
                    .expect("Failed to create tokio runtime")
                .block_on(async { #(#stmts);* });
            })
        }
    }
    .into_token_stream()
    .into()
}

fn parse_worker_threads(args: AttributeArgs) -> Result<usize, syn::Error> {
    #[allow(clippy::never_loop)]
    for arg in args {
        match arg {
            syn::Meta::NameValue(namevalue) => {
                let ident = namevalue
                    .path
                    .get_ident()
                    .ok_or_else(|| syn::Error::new_spanned(&namevalue, "Must have specified ident"))?
                    .to_string()
                    .to_lowercase();
                let lit = match &namevalue.value {
                    syn::Expr::Lit(syn::ExprLit { lit, .. }) => lit,
                    expr => return Err(syn::Error::new_spanned(expr, "Must be a literal")),
                };
                match ident.as_str() {
                    "worker_threads" => {
                        return match lit {
                            Lit::Int(i) => match i.base10_parse::<usize>() {
                                Ok(value) => Ok(value),
                                Err(e) => Err(syn::Error::new(
                                    syn::spanned::Spanned::span(lit),
                                    format!("Failed to parse value of `{ident}` as integer: {e}"),
                                )),
                            },
                            other => Err(syn::Error::new_spanned(other, "Could not parse worker_threads as int")),
                        }
                    },
                    name => {
                        let msg = format!(
                            "Unknown attribute {name} is specified; expected one of: `flavor`, `worker_threads`, `start_paused`, `crate`, `unhandled_panic`",
                        );
                        return Err(syn::Error::new_spanned(namevalue, msg));
                    },
                }
            },
            other => {
                return Err(syn::Error::new_spanned(other, "Unknown attribute inside the macro"));
            },
        }
    }
    Ok(4)
}

type AttributeArgs = syn::punctuated::Punctuated<syn::Meta, syn::Token![,]>;
#[proc_macro_attribute]
pub fn sim_test(args: TokenStream, input: TokenStream) -> TokenStream {
    let input: syn::ItemFn = match syn::parse2(input.clone().into()) {
        Ok(it) => it,
        Err(e) => return e.into_compile_error().into(),
    };
    let workers = match AttributeArgs::parse_terminated
        .parse2(args.into())
        .and_then(parse_worker_threads)
    {
        Ok(r) => r,
        Err(e) => return e.into_compile_error().into(),
    };
    format_tokens(input, true, workers)
}

#[proc_macro_attribute]
pub fn live_test(args: TokenStream, input: TokenStream) -> TokenStream {
    let input: syn::ItemFn = match syn::parse2(input.clone().into()) {
        Ok(it) => it,
        Err(e) => return e.into_compile_error().into(),
    };
    let workers = match AttributeArgs::parse_terminated
        .parse2(args.into())
        .and_then(parse_worker_threads)
    {
        Ok(r) => r,
        Err(e) => return e.into_compile_error().into(),
    };
    format_tokens(input, false, workers)
}
