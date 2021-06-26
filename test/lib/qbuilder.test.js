const Q = require('../../lib/qbuilder')
const Lab = require('@hapi/lab')
const lab = exports.lab = Lab.script()
const { describe, before, after, it } = lab

describe('qbuilder', () => {
  /*
  it('', async () => {
    const ins_node = insert({
      into: 'users',
      values: { id: 'aaa', email: 'rr@voxgig.com' }
    })

    console.dir(ins_node), { depth: 8 }
    console.dir(toSql(ins_node), { depth: 4 })
  })
  */

  it('', async () => {
    const sel_node = select({
      columns: ['id', 'email'],
      from: 'users',
      where: exists(select({
        columns: '*',
        from: 'users',
        where: { id: 'aaaa' }
      })),
      limit: 1,
      offset: 5,
      order_by: { email: -1, id: 1 }
    })

    console.dir(sel_node, { depth: null })
    console.dir(toSql(sel_node), { depth: 4 })
  })
})


const Assert = require('assert')

/*
type insert_t = {
  whatami$: "insert_t";
  into$: string;
  values$: object;
}
*/
function insert(args) {
  const { into, values } = args

  Assert.strictEqual(typeof into, 'string', 'into')
  Assert(values, 'values')

  return {
    whatami$: 'insert_t',
    into$: into,
    values$: values
  }
}

/*
type expr_t = [
| select_t
| expr_null_t
| expr_not_t
| expr_eq_t
| expr_exists_t
]

type expr_null_t = {
  expr$: expr_t
}

type expr_not_t = {
  expr$: expr_t
}

type expr_in_t = {
  unsafe_args$: 'a array
}

type expr_eq_t = {
  column$: string;
  value$: 'a
}

type expr_exists_t = {
  whatami$: 'expr_exists_t';
  expr$: select_t
}
*/

function exists(expr) {
  Assert(expr, 'expr')
  Assert.strictEqual(expr.whatami$, 'select_t')

  return {
    whatami$: 'expr_exists_t',
    expr$: expr
  }
}

/*
  type select_t = {
    from$: string;
    columns$: string array;
    offset$: int;
    limit$: int;
    order_by$: { [string]: [ -inf..-1 | 0..inf | 'desc' | 'asc' ] }
  }
 */

function where(obj) {
  if ('string' === typeof obj.whatami$) {
    return obj
  }

  const kvs = Object.keys(obj).map(k => [k, obj[k]])

  const expr = kvs.reduce((acc, [colname, x]) => {
    const eq_expr = 'string' === typeof x.whatami$ ? x : {
      whatami$: 'expr_eq_t',
      column$: colname,
      value$: x
    }

    if (!acc) {
      return eq_expr
    }

    return {
      whatami$: 'expr_and_t',
      lexpr$: acc,
      rexpr$: eq_expr
    }
  }, null)

  return expr 
}

function select(args) {
  const {
    from,
    columns = '*',
    offset = null,
    limit = null,
    order_by = null,
    where: w
  } = args

  return {
    whatami$: 'select_t',
    from$: from,
    columns$: columns,
    offset$: offset,
    limit$: limit,
    order_by$: order_by,
    where$: where(w)
  }
}

function sqlOfOrderBy(node) {
  let sql = ''
  let bindings = []


  let first_pair = true

  for (const order_col in node) {
    const order_val = node[order_col]


    let order
    
    if ('string' === typeof order_val) {
      if ('desc' === order_val.toLowerCase()) {
        order = 'desc'
      } else if ('asc' === order_val.toLowerCase()) {
        order = 'asc'
      } else {
        throw new Error(`Unknown order: ${order_val}`)
      }
    } else if ('number' === typeof order_val) {
      order = 0 <= order_val ? 'asc' : 'desc'
    } else {
      throw new Error('order must be a number or a string')
    }

    if (!first_pair) {
      sql += ', '
    }

    sql += '?? ' + order
    bindings.push(order_col)

    first_pair = false
  }


  return { sql, bindings }
}

function sqlOfExpr(node) {
  if ('select_t' === node.whatami$) {
    return sqlOfSelect(node)
  }

  if ('expr_eq_t' === node.whatami$) {
    return {
      sql: '?? = ?',
      bindings: [node.column$, node.value$]
    }
  }

  if ('expr_exists_t' === node.whatami$) {
    return sqlOfExistsExpr(node)
  }

  if ('expr_and_t' === node.whatami$) {
    const { lexpr$, rexpr$ } = node

    const lexpr_sql = sqlOfExpr(lexpr$)
    const rexpr_sql = sqlOfExpr(rexpr$)

    return {
      sql: lexpr_sql.sql + ' and ' + rexpr_sql.sql,
      bindings: [].concat(lexpr_sql.bindings, rexpr_sql.bindings)
    }
  }

  Assert.fail(`Unknown expression type: "${node.whatami$}"`)
}

function sqlOfSelect(node) {
  Assert.strictEqual(node.whatami$, 'select_t')

  const {
    from$,
    columns$ = '*',
    offset$ = null,
    limit$ = null,
    order_by$ = null,
    where$ = null
  } = node


  let bindings = []
  let sql = ''


  sql += 'select '


  if ('*' === columns$) {
    sql += '*'
  } else {
    const col_placeholders = columns$.map(_ => '??')
    sql += col_placeholders.join(', ')

    bindings = bindings.concat(columns$)
  }


  sql += ' from ??'
  bindings.push(from$)


  if (null != where$) {
    const where = sqlOfExpr(where$)

    sql += ' where ' + where.sql
    bindings = bindings.concat(where.bindings)
  }


  if (null != order_by$) {
    const order_q = sqlOfOrderBy(order_by$)

    sql += ' order by ' + order_q.sql
    bindings = bindings.concat(order_q.bindings)
  }


  if (null != limit$) {
    sql += ' limit ?'
    bindings.push(limit$)
  }


  if (null != offset$) {
    sql += ' offset ?'
    bindings.push(offset$)
  }


  return { sql, bindings }
}

function sqlOfExistsExpr(node) {
  Assert.strictEqual(node.whatami$, 'expr_exists_t')

  const { expr$ } = node
  const q = sqlOfSelect(expr$)

  return {
    sql: 'exists (' + q.sql + ')',
    bindings: q.bindings
  }
}

function sqlOfInsert(node) {
  Assert.strictEqual(node.whatami$, 'insert_t', 'node.whatami$')

  const { into$, values$ } = node

  const col_names = Object.keys(values$)
  const col_vals = Object.values(values$)


  let bindings = []
  let sql = ''


  sql += 'insert into ?? '
  bindings.push(into$)


  const col_placeholders = col_names.map(_ => '??')
  sql += '(' + col_placeholders.join(', ') + ') '
  bindings = bindings.concat(col_names)


  const val_placeholders = col_vals.map(_ => '?')
  sql += 'values (' + val_placeholders.join(', ') + ') '
  bindings = bindings.concat(col_vals)


  return { sql, bindings }
}

function toSql(node) {
  Assert(node, 'node')

  if ('insert_t' === node.whatami$) {
    return sqlOfInsert(node)
  }

  if ('select_t' === node.whatami$) {
    return sqlOfSelect(node)
  }

  Assert.fail(`Unknown node type: ${node.whatami$}`)
}

