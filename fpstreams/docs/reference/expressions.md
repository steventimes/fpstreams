# Expressions

`item` and `fitem` build scalar placeholder expressions for flows. `col()`,
`lit()`, `when()`, and `coalesce()` build record expressions for Rows and for
record-capable Flow operations.

~~~python
from fpstreams import col, flow, item

numbers = flow(range(5)).filter(item % 2 == 0).map(item * 10)
active = flow([{"status": "active"}]).filter(col("status") == "active").select("status")
~~~

Scalar `filter()` and `map()` calls keep an ordinary Flow. A nonconflicting
record method such as `select()` enters Rows automatically; call `.rows()` first
when you need a conflicting relational method such as Rows `where()` with field
equalities.

## Building row expressions

| Call | Meaning |
| --- | --- |
| `col(selector)` | Read a field, index, attribute, nested path, or callable selector |
| `lit(value)` | Return the same value for every row |
| `when(condition, then, otherwise)` | Choose a value from a condition |
| `coalesce(*values)` | Return the first value that is not `None` |

Row-expression methods such as `cast()`, `isin()`, `fill_null()`, `lower()`, and
`contains()` return new expressions; they do not read rows immediately.

## Scalar expressions

::: fpstreams.Expr
    options:
      members_order: source
      show_root_heading: true
      show_source: false

## Row expressions

::: fpstreams.RowExpr
    options:
      members_order: source
      show_root_heading: true
      show_source: false
