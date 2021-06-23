class Q {
  static insertstm(args) {
    const { into, values } = args

    const col_names = Object.keys(values)
    const col_vals = Object.values(values)


    let bindings = []
    let sql = ''


    sql += 'insert into ?? '
    bindings.push(into)


    const col_placeholders = col_names.map(_ => '??')
    sql += '(' + col_placeholders.join(', ') + ') '
    bindings = bindings.concat(col_names)


    const val_placeholders = col_vals.map(_ => '?')
    sql += 'values (' + val_placeholders.join(', ') + ') '
    bindings = bindings.concat(col_vals)


    return { sql, bindings }
  }

  static wherestm(args) {
    const { where } = args
    const update_all = 0 === Object.keys(where).length


    let sql = ''
    let bindings = []


    if (update_all) {
      sql += '1'
    } else {
      let first_where = true

      for (const where_col in where) {
        const where_val = where[where_col]

        if (!first_where) {
          sql += ' and '
        }

        if (Array.isArray(where_val)) {
          const val_placeholders = where_val.map(_ => '?').join(', ')

          if (0 === val_placeholders.length) {
            sql += '0'
          } else {
            sql += '?? in (' + val_placeholders + ')'

            bindings.push(where_col)
            bindings = bindings.concat(where_val)
          }
        } else {
          if (null == where_val) {
            sql += '?? is null'
          } else {
            sql += '?? = ?'
          }

          bindings.push(where_col)
          bindings.push(where_val)
        }

        first_where = false
      }
    }

    return { sql, bindings }
  }

  static updatestm(args) {
    const { table, set, where } = args


    let bindings = []
    let sql = ''


    sql += 'update ?? '
    bindings.push(table)

    sql += 'set '


    let first_set = true

    for (const set_col in set) {
      const set_val = set[set_col]

      if (!first_set) {
        sql += ', '
      }

      sql += '?? = ?'
      bindings.push(set_col)
      bindings.push(set_val)

      first_set = false
    }

    sql += ' '


    const where_q = Q.wherestm({ where })

    sql += 'where ' + where_q.sql
    bindings = bindings.concat(where_q.bindings)


    return { sql, bindings }
  }

  static orderbystm(args) {
    const { order_by } = args


    let sql = ''
    let bindings = []


    let first_pair = true

    for (const order_col in order_by) {
      const order_val = order_by[order_col]


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

  static selectstm(args) {
    const {
      from,
      columns = '*',
      where = null,
      offset = null,
      limit = null,
      order_by = null
    } = args


    let bindings = []
    let sql = ''


    sql += 'select '


    if ('*' === columns) {
      sql += '*'
    } else {
      const col_placeholders = columns.map(_ => '??')
      sql += col_placeholders.join(', ')

      bindings = bindings.concat(columns)
    }


    sql += ' from ??'
    bindings.push(from)


    if (null != where) {
      const where_q = Q.wherestm({ where })

      sql += ' where ' + where_q.sql
      bindings = bindings.concat(where_q.bindings)
    }


    if (null != order_by) {
      const order_q = Q.orderbystm({ order_by })

      sql += ' order by ' + order_q.sql
      bindings = bindings.concat(order_q.bindings)
    }


    if (null != limit) {
      sql += ' limit ?'
      bindings.push(limit)
    }


    if (null != offset) {
      sql += ' offset ?'
      bindings.push(offset)
    }


    return { sql, bindings }
  }

  static deletestm(args) {
    const { from, where = null, limit = null } = args

    let sql = ''
    let bindings = []

    sql += 'delete from ?? '
    bindings.push(from)

    if (null != where) {
      const where_q = Q.wherestm({ where })

      sql += 'where ' + where_q.sql
      bindings = bindings.concat(where_q.bindings)
    }

    if (null != limit) {
      sql += ' limit ?'
      bindings.push(limit)
    }

    return { sql, bindings }
  }
}

module.exports = Q
