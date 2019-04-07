package com.googlecode.cqengine.query.parser.sql2;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chuxiaofeng
 */
public interface Utils {

    static void error(String msg)  {
        // throw new SQLFeatureNotSupportedException(msg);
        throw new RuntimeException(msg);
    }
    static void un_support(String sql)  {
        error("Unsupported sql: " + sql);
    }
    static void un_support(SQLExpr expr)  {
        error("Unsupported sql expr: " + expr);
    }

    static void assert_true(boolean cond, String sql) {
        if (!cond) error("Unsupport sql ==> " + sql);
    }

    static void checkQuery(MySqlSelectQueryBlock qry, String sql) {
        assert_true(!qry.isStraightJoin(), sql);
        assert_true(qry.getProcedureName() == null, sql);
        assert_true(!qry.isLockInShareMode(), sql);
        assert_true(qry.getForcePartition() == null, sql);
        assert_true(qry.getInto() == null, sql);
        assert_true(qry.getGroupBy() == null, sql);
        assert_true(!qry.isParenthesized(), sql);
        assert_true(!qry.isForUpdate(), sql);

        // TODO
        // qry.getDistionOption()

    }

    ////////////////////////////////////////////////////////////////////////////////////////////////


    static boolean isUnion(SQLQueryExpr sqlExpr) {
        return sqlExpr.getSubQuery().getQuery() instanceof SQLUnionQuery;
    }

    static boolean isJoin(SQLQueryExpr sqlExpr, String sql) {
        MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery();
        return query.getFrom() instanceof SQLJoinTableSource && sql.toLowerCase().contains("join");
    }

    static boolean isCond(SQLBinaryOpExpr expr) {
        SQLExpr l = expr.getLeft();
        if (l instanceof SQLMethodInvokeExpr) return false;
        return l instanceof SQLIdentifierExpr ||
                l instanceof SQLPropertyExpr ||
                l instanceof SQLVariantRefExpr/*?*/;
    }

    static boolean isFromJoinOrUnionTable(SQLExpr expr) {
        SQLObject temp = expr;
        AtomicInteger counter = new AtomicInteger(10);
        while (temp != null &&
                !(expr instanceof SQLSelectQueryBlock) &&
                !(expr instanceof SQLJoinTableSource) && !(expr instanceof SQLUnionQuery) && counter.get() > 0) {
            counter.decrementAndGet();
            temp = temp.getParent();
            if (temp instanceof SQLSelectQueryBlock) {
                SQLTableSource from = ((SQLSelectQueryBlock) temp).getFrom();
                if (from instanceof SQLJoinTableSource || from instanceof SQLUnionQuery) {
                    return true;
                }
            }
            if (temp instanceof SQLJoinTableSource || temp instanceof SQLUnionQuery) {
                return true;
            }
        }
        return false;
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////
    
    
    static Object expr2Object(SQLExpr expr) {
        return expr2Object(expr, "");
    }

    static Object expr2Object(SQLExpr expr, String charWithQuote) {
        Object value = null;
        if (expr instanceof SQLNumericLiteralExpr) {
            value = ((SQLNumericLiteralExpr) expr).getNumber();
        } else if (expr instanceof SQLCharExpr) {
            value = charWithQuote + ((SQLCharExpr) expr).getText() + charWithQuote;
        } else if (expr instanceof SQLIdentifierExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLPropertyExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLVariantRefExpr) {
            value = expr.toString();
        } else if (expr instanceof SQLAllColumnExpr) {
            value = "*";
        } else if (expr instanceof SQLValuableExpr) {
            value = ((SQLValuableExpr) expr).getValue();
        } else {
            //throw new SqlParseException("can not support this type " + expr.getClass());
        }
        return value;
    }

}
