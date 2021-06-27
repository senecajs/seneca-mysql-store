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
      /*
      where: not(exists(select({
        columns: '*',
        from: 'users',
        where: { id: 'aaaa', email: null, favorite_fruits: ['oranges', 'melon'] }
      }))),
      */
      where: and(
        { id: 'zzzz', email: 'rr@voxgig.com' },

        not(exists(select({
          columns: '*',
          from: 'users',
          where: { favorite_fruits: ['oranges'] }
        })))
      ),
      limit: 1,
      offset: 5,
      order_by: { email: -1, id: 1 }
    })

    console.dir(sel_node, { depth: null })
    console.dir(toSql(sel_node), { depth: 4 })
  })
})


const Assert = require('assert')

class QNode {}

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

  return new InsertStm({
    into$: into,
    values$: values
  })
}

class InsertStm extends QNode {
  constructor(args) {
    super(args)

    this.into$ = args.into$
    this.values$ = args.values$
  }
}

class Op extends QNode {}

class UnaryOp extends Op {}

class NotOp extends UnaryOp {
  constructor(args) {
    super(args)

    this.expr$ = args.expr$
  }
}

class ExistsOp extends UnaryOp {
  constructor(args) {
    super(args)


    Assert(args, 'args')
    Assert(args.expr$ instanceof SelectStm, 'expr$')

    this.expr$ = args.expr$
  }
}

class InOp extends UnaryOp {
  constructor(args) {
    super(args)

    Assert(args, 'args')
    Assert(Array.isArray(args.values$), 'args.values$')

    this.column$ = args.column$
    this.values$ = args.values$
  }
}

class NullOp extends UnaryOp {
  constructor(args) {
    super(args)

    this.column$ = args.column$
  }
}

class BinaryOp extends Op {}

class EqOp extends BinaryOp {
  constructor(args) {
    super(args)

    this.column$ = args.column$
    this.value$ = args.value$
  }
}

class AndOp extends BinaryOp {
  constructor(args) {
    super(args)

    this.lexpr$ = args.lexpr$
    this.rexpr$ = args.rexpr$
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
  return new ExistsOp({ expr$: expr })
}

function not(expr) {
  return new NotOp({ expr$: expr })
}

function and(left_val, right_val) {
  const ensureExpr = v => v instanceof QNode ? v : ObjectExpr.ofObject(v)

  const lexpr = ensureExpr(left_val)
  const rexpr = ensureExpr(right_val)

  return new AndOp({ lexpr$: lexpr, rexpr$: rexpr })
}

class ObjectExpr extends QNode {
  constructor(args) {
    super(args)

    this.expr$ = args.expr$
  }

  static ofObject(obj) {
    const kvs = Object.keys(obj).map(k => [k, obj[k]])

    const expr = kvs.reduce((acc, [colname, x]) => {
      let subexpr

      if (x instanceof QNode) {
        subexpr = x
      } else {
        if (null == x) {
          subexpr = new NullOp({ column$: colname })
        } else if (Array.isArray(x)) {
          subexpr = new InOp({ column$: colname, values$: x })
        } else {
          subexpr = new EqOp({ column$: colname, value$: x })
        }
      }

      if (!acc) {
        return subexpr
      }

      return new AndOp({ lexpr$: acc, rexpr$: subexpr })
    }, null)

    return new ObjectExpr({ expr$: expr })
  }
}

function where(obj) {
  if (obj instanceof Op) {
    return obj
  }

  if (obj instanceof QNode) {
    throw new Error(`The where-object is of the unsupported type`)
  }

  return ObjectExpr.ofObject(obj)
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
class SelectStm {
  constructor(args) {
    this.from$ = args.from$
    this.columns$ = args.column$ || '*'
    this.offset$ = args.offset$ || null
    this.limit$ = args.limit$ || null
    this.order_by$ = args.order_by$ || null
    this.where$ = args.where$ || null
  }
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

  return new SelectStm({
    from$: from,
    columns$: columns,
    offset$: offset,
    limit$: limit,
    order_by$: order_by,
    where$: where(w)
  })
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
  if (node instanceof SelectStm) {
    return sqlOfSelect(node)
  }

  if (node instanceof NotOp) {
    const subexpr_q = sqlOfExpr(node.expr$)

    return {
      sql: 'not ' + subexpr_q.sql,
      bindings: subexpr_q.bindings
    }
  }

  if (node instanceof EqOp) {
    return {
      sql: '?? = ?',
      bindings: [node.column$, node.value$]
    }
  }

  if (node instanceof NullOp) {
    return {
      sql: '?? is null',
      bindings: [node.column$]
    }
  }

  if (node instanceof InOp) {
    const tuple = node.values$.map(_ => '?')

    return {
      sql: '?? in (' + tuple.join(', ') + ')',
      bindings: [node.column$, ...node.values$]
    }
  }

  if (node instanceof ExistsOp) {
    return sqlOfExistsExpr(node)
  }

  if (node instanceof AndOp) {
    const { lexpr$, rexpr$ } = node

    const lexpr_sql = sqlOfExpr(lexpr$)
    const rexpr_sql = sqlOfExpr(rexpr$)

    return {
      sql: lexpr_sql.sql + ' and ' + rexpr_sql.sql,
      bindings: [].concat(lexpr_sql.bindings, rexpr_sql.bindings)
    }
  }

  if (node instanceof ObjectExpr) {
    return sqlOfExpr(node.expr$)
  }

  Assert.fail(`Unknown expression type: "${node && node.constructor}"`)
}

function sqlOfSelect(node) {
  Assert(node instanceof SelectStm, 'node')

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
  Assert(node instanceof ExistsOp, 'node')

  const { expr$ } = node
  const q = sqlOfSelect(expr$)

  return {
    sql: 'exists (' + q.sql + ')',
    bindings: q.bindings
  }
}

function sqlOfInsert(node) {
  Assert(node instanceof InsertStm, 'node')

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

  if (node instanceof InsertStm) {
    return sqlOfInsert(node)
  }

  if (node instanceof SelectStm) {
    return sqlOfSelect(node)
  }

  Assert.fail(`Unknown node type: ${node && node.constructor}`)
}

