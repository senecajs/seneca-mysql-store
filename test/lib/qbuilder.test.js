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
      from: 'users',
      columns: ['id', 'email'],
      limit: 1,
      offset: 5,
      order_by: { email: -1, id: 1 }
    })

    console.dir(sel_node), { depth: 8 }
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
type expr_t = 
| select_t
| expr_null_t
| expr_not_t
| expr_eq_t
| expr_exists_t

type expr_null_t = {
  expr$: expr_t
}

type expr_not_t = {
  expr$: expr_t
}

type expr_eq_t = {
  column$: string;
  unsafe_arg$: a'
}

type expr_exists_t = {
  expr$: select_t
}
*/

/*
  type select_t = {
    from$: string;
    columns$: string array;
    offset$: int;
    limit$: int;
    order_by$: { [string]: [ -inf..-1 | 0..inf | 'desc' | 'asc' ] }
  }
 */

function select(args) {
  const {
    from,
    columns = '*',
    offset = null,
    limit = null,
    order_by = null
  } = args

  return {
    whatami$: 'select_t',
    from$: from,
    columns$: columns,
    offset$: offset,
    limit$: limit,
    order_by$: order_by
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

function sqlOfSelect(node) {
  const {
    from$,
    columns$ = '*',
    offset$ = null,
    limit$ = null,
    order_by$ = null
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

/*
function sqlOfExpr(node) {
  if ('select_t' === node.whatami$) {
    throw new Error('select_t not implemented')
  }

  if ('expr_null_t' === node.whatami$) {
    throw new Error('not implemented')
  }

  if ('expr_eq_t' === node.whatami$) {
    throw new Error('not implemented')
  }

  if ('expr_exists_t' === node.whatami$) {
    throw new Error('not implemented')
  }

  throw new Error(`Unknown expression type: ${node.whatami$}`)
}
*/

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

