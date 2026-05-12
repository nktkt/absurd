//! Procedural macros for `absurd-sdk`.
//!
//! The `#[absurd::task]` attribute turns an `async fn` into a typed task
//! definition reusable with [`Client::register_task`]. The function signature
//! `async fn name(params: P) -> Result<R>` is preserved verbatim; the macro
//! emits a sibling `pub fn name_task() -> TaskDefinition<P, R>` that builds
//! the definition from the function.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, FnArg, GenericArgument, ItemFn, LitStr, Pat, PatType, PathArguments,
    ReturnType, Token, Type,
};

/// Attribute macro: `#[absurd::task]` or `#[absurd::task(name = "...", queue = "...")]`.
///
/// The annotated `async fn` must take exactly one parameter (the task params)
/// and return `absurd_sdk::Result<R>` (or any `Result<R, _>`). The macro emits:
///
/// 1. The original function, unchanged.
/// 2. A `pub fn <orig>_task() -> TaskDefinition<P, R>` helper that wires the
///    function up as a task definition.
#[proc_macro_attribute]
pub fn task(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as TaskArgs);
    let input = parse_macro_input!(item as ItemFn);
    match expand_task(args, input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[derive(Default)]
struct TaskArgs {
    name: Option<LitStr>,
    queue: Option<LitStr>,
    max_attempts: Option<syn::LitInt>,
}

impl Parse for TaskArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = TaskArgs::default();
        while !input.is_empty() {
            let key: syn::Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "name" => args.name = Some(input.parse()?),
                "queue" => args.queue = Some(input.parse()?),
                "max_attempts" => args.max_attempts = Some(input.parse()?),
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("unknown #[task] argument {:?}", other),
                    ))
                }
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        Ok(args)
    }
}

fn expand_task(args: TaskArgs, input: ItemFn) -> syn::Result<TokenStream2> {
    let fn_name = &input.sig.ident;
    let vis = &input.vis;
    let task_name = args
        .name
        .map(|lit| lit.value())
        .unwrap_or_else(|| fn_name.to_string());
    let queue = args.queue.map(|lit| lit.value());
    let max_attempts = args.max_attempts;

    if input.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            input.sig.fn_token,
            "#[absurd::task] requires an async fn",
        ));
    }
    if input.sig.inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            &input.sig.inputs,
            "#[absurd::task] expects exactly one parameter (the task params)",
        ));
    }
    let param_ty = match input.sig.inputs.first().unwrap() {
        FnArg::Typed(PatType { ty, pat, .. }) => match pat.as_ref() {
            Pat::Ident(_) | Pat::Wild(_) => (*ty).clone(),
            _ => {
                return Err(syn::Error::new_spanned(
                    pat,
                    "#[absurd::task] parameter must be a plain identifier",
                ))
            }
        },
        FnArg::Receiver(r) => {
            return Err(syn::Error::new_spanned(
                r,
                "#[absurd::task] does not support self parameters",
            ))
        }
    };

    let ok_ty = extract_result_ok(&input.sig.output)?;

    let builder_name = format_ident!("{}_task", fn_name);
    let queue_call = match queue {
        Some(q) => quote! { def = def.on_queue(#q); },
        None => quote! {},
    };
    let max_call = match max_attempts {
        Some(n) => quote! { def = def.with_max_attempts(#n); },
        None => quote! {},
    };

    Ok(quote! {
        #input

        #[allow(non_snake_case)]
        #vis fn #builder_name() -> ::absurd_sdk::TaskDefinition<#param_ty, #ok_ty> {
            let mut def = ::absurd_sdk::task::<_, #param_ty, #ok_ty, _>(
                #task_name,
                |params: #param_ty| async move { #fn_name(params).await },
            );
            #queue_call
            #max_call
            def
        }
    })
}

/// Pull `T` out of a `Result<T, _>` return type.
fn extract_result_ok(ret: &ReturnType) -> syn::Result<Box<Type>> {
    let ty = match ret {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                ret,
                "#[absurd::task] requires a Result return type",
            ))
        }
        ReturnType::Type(_, ty) => ty,
    };
    let path = match ty.as_ref() {
        Type::Path(tp) => &tp.path,
        _ => {
            return Err(syn::Error::new_spanned(
                ty,
                "expected a Result<T, _> return type",
            ))
        }
    };
    let last = path
        .segments
        .last()
        .ok_or_else(|| syn::Error::new_spanned(ty, "expected a Result<T, _> return type"))?;
    if last.ident != "Result" {
        return Err(syn::Error::new_spanned(
            ty,
            "expected the return type to be Result<T, _> (e.g. absurd_sdk::Result<T>)",
        ));
    }
    let args = match &last.arguments {
        PathArguments::AngleBracketed(args) => args,
        _ => {
            return Err(syn::Error::new_spanned(
                ty,
                "Result must have a generic parameter (Result<T> or Result<T, E>)",
            ))
        }
    };
    for arg in args.args.iter() {
        if let GenericArgument::Type(t) = arg {
            return Ok(Box::new(t.clone()));
        }
    }
    Err(syn::Error::new_spanned(
        ty,
        "Result must have at least one type parameter",
    ))
}
