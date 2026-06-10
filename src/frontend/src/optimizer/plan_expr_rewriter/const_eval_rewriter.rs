// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::FutureExt;
use risingwave_common::row::OwnedRow;
use risingwave_expr::expr::build_from_prost;

use crate::error::RwError;
use crate::expr::{
    Expr, ExprImpl, ExprRewriter, ExprVisitor, Literal, UserDefinedFunction, default_rewrite_expr,
};

pub(crate) struct ConstEvalRewriter {
    pub(crate) error: Option<RwError>,
}
impl ExprRewriter for ConstEvalRewriter {
    fn rewrite_expr(&mut self, expr: ExprImpl) -> ExprImpl {
        if self.error.is_some() {
            return expr;
        }
        if !expr.is_const() {
            return default_rewrite_expr(self, expr);
        }
        // Never fold a whole constant node that contains a UDF which cannot be evaluated in the
        // frontend (e.g. an external arrow-flight UDF, or one whose runtime fails to build here).
        // Instead, recurse so that independent constant sub-expressions (such as `1 / 0`) are
        // still folded — and their errors still surface at planning time — while the
        // non-evaluable UDF is simply left unchanged.
        if contains_non_frontend_evaluable_udf(&expr) {
            return default_rewrite_expr(self, expr);
        }
        match expr.try_fold_const() {
            Some(Ok(datum)) => Literal::new(datum, expr.return_type()).into(),
            Some(Err(e)) => {
                self.error = Some(e);
                expr
            }
            // Unreachable given `is_const()` above, but recurse to be safe.
            None => default_rewrite_expr(self, expr),
        }
    }
}

/// Returns `true` if `expr` contains a user-defined function that cannot be evaluated in the
/// frontend at planning time.
fn contains_non_frontend_evaluable_udf(expr: &ExprImpl) -> bool {
    let mut finder = NonEvaluableUdfFinder { found: false };
    finder.visit_expr(expr);
    finder.found
}

struct NonEvaluableUdfFinder {
    found: bool,
}

impl ExprVisitor for NonEvaluableUdfFinder {
    fn visit_user_defined_function(&mut self, func_call: &UserDefinedFunction) {
        if self.found {
            return;
        }
        if !udf_is_frontend_evaluable(func_call) {
            self.found = true;
            return;
        }
        // The UDF itself is evaluable; still inspect its arguments, which may contain a
        // nested non-evaluable UDF.
        func_call.args.iter().for_each(|e| self.visit_expr(e));
    }
}

/// Whether a UDF can be built and evaluated in-process in the frontend, without any network
/// access. This is intentionally conservative: a `false` only costs a folding opportunity,
/// whereas a wrong `true` would make `try_fold_const` fail and propagate the error.
fn udf_is_frontend_evaluable(func_call: &UserDefinedFunction) -> bool {
    let catalog = &func_call.catalog;

    // External arrow-flight UDFs require a network connection (and would make `eval_row`
    // async, panicking `now_or_never`). See the `external` UDF impl `match_fn`.
    let is_external =
        catalog.link.is_some() && matches!(catalog.language.as_str(), "python" | "java" | "");
    if is_external {
        return false;
    }

    // Probe whether the UDF runtime actually builds *and* evaluates synchronously in this
    // frontend process. Use NULL literal arguments of the declared types so we only test the
    // UDF runtime itself and do not recursively build nested (possibly external) UDF arguments.
    //
    // The eval readiness check is essential: `try_fold_const` evaluates via
    // `eval_row(..).now_or_never().expect("constant expression should not be async")`, which
    // would panic for a runtime whose evaluation is not immediately ready (e.g. one that awaits
    // I/O). We poll the probe once and only treat the UDF as evaluable if the future is ready.
    // A `Some(Err(..))` (the UDF erroring on NULL input) still counts as evaluable — that means
    // the runtime ran synchronously; genuine evaluation errors on real arguments are handled by
    // `try_fold_const` and propagated at planning time as before.
    let probe_args: Vec<ExprImpl> = catalog
        .arg_types
        .iter()
        .map(|t| Literal::new(None, t.clone()).into())
        .collect();
    let probe = UserDefinedFunction::new(catalog.clone(), probe_args);
    let Ok(node) = probe.try_to_expr_proto() else {
        return false;
    };
    let Ok(backend_expr) = build_from_prost(&node) else {
        return false;
    };
    backend_expr
        .eval_row(&OwnedRow::empty())
        .now_or_never()
        .is_some()
}
