package com.googlecode.cqengine.query.parser.sql2;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlOrderingExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;

import java.util.*;
import java.util.stream.Collectors;

import static com.googlecode.cqengine.query.parser.sql2.SQLPreParser.ConditionGroup.LogicOp.AND;
import static com.googlecode.cqengine.query.parser.sql2.Utils.*;
import static java.util.stream.Collectors.toList;


/**
 * @author chuxiaofeng
 */
public class SQLPreParser<O> {


    ////////////////////////////////////////////////////////////////////////////////

    MySqlSelectQueryBlock getSelectQuery(String sql) {
        List<SQLStatement> stmtLst = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        assert_true(stmtLst != null && stmtLst.size() == 1, sql);
        assert stmtLst != null;
        SQLStatement stmt = stmtLst.get(0);
        assert_true(stmt instanceof SQLSelectStatement, sql);
        //noinspection ConstantConditions
        SQLSelectStatement selectStmt = ((SQLSelectStatement) stmt);
        SQLSelect select = selectStmt.getSelect();
        MySqlSelectQueryBlock qry = ((MySqlSelectQueryBlock) select.getQuery());
        checkQuery(qry, sql);
        return qry;
    }


    ////////////////////////////////////////////////////////////////////////////////

    public Select parseSimpleSelect(String sql) {
        Object r = parse(sql);
        if (r instanceof Select) {
            return ((Select) r);
        }
        un_support(sql);
        return null;
    }

    ////////////////////////////////////////////////////////////////////////////////


    Object parse(String sql) {
        String firstWord = sql.substring(0, sql.indexOf(' '));
        switch (firstWord.toUpperCase()) {
            case "SELECT":
                SelectParser selectParser = new SelectParser();
                SQLQueryExpr sqlExpr = (SQLQueryExpr) SQLUtils.toMySqlExpr(sql);
                if(isUnion(sqlExpr)) {
                    return selectParser.parseMultiSelect((SQLUnionQuery) sqlExpr.getSubQuery().getQuery());
                } else if(isJoin(sqlExpr,sql)) {
                    return selectParser.parseJoinSelect(sqlExpr);
                } else {
                    return selectParser.parseSelect((MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery());
                }
            case "DELETE":
            case "UPDATE":
            case "INSERT":
            case "SHOW":
            default: un_support(sql);
        }
        return null;
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
    static class SelectParser {
        public Select parseSelect(String sql) {
            String firstWord = sql.substring(0, sql.indexOf(' '));
            switch (firstWord.toUpperCase()) {
                case "SELECT":
                    SQLQueryExpr sqlExpr = (SQLQueryExpr) SQLUtils.toMySqlExpr(sql);
                    if(isUnion(sqlExpr)) un_support(sql);
                    else if(isJoin(sqlExpr,sql)) un_support(sql);
                    else return parseSelect((MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery());
                case "DELETE":
                case "UPDATE":
                case "INSERT":
                case "SHOW":
                default: un_support(sql);
            }
            return null;
        }

        public MultiQuerySelect parseMultiSelect(SQLUnionQuery query) {
            Select left = parseSelect((MySqlSelectQueryBlock) query.getLeft());
            Select right = parseSelect((MySqlSelectQueryBlock) query.getRight());
            return new MultiQuerySelect(query.getOperator(), left, right);
        }

        public JoinSelect parseJoinSelect(SQLQueryExpr sqlExpr)  {
            MySqlSelectQueryBlock query = (MySqlSelectQueryBlock) sqlExpr.getSubQuery().getQuery();

            List<From> joinedFrom = findJoinedFrom(query.getFrom());
            if (joinedFrom.size() != 2) {
                error("currently supports only 2 tables join");
            }

            JoinSelect joinSelect = createBasicJoinSelectAccordingToTableSource((SQLJoinTableSource) query.getFrom());

            String firstTableAlias = joinedFrom.get(0).alias;
            String secondTableAlias = joinedFrom.get(1).alias;

            Map<String, ConditionGroup> aliasToWhere = splitAndFindWhere(query.getWhere(), firstTableAlias, secondTableAlias);
            Map<String, List<SQLSelectOrderByItem>> aliasToOrderBy = splitAndFindOrder(query.getOrderBy(), firstTableAlias, secondTableAlias);
            List<Condition> connectedConditions = getConditionsFlatten(joinSelect.connectedWhere);
            joinSelect.connectedConditions = connectedConditions;
            fillTableSelectedJoin(joinSelect.firstTable, query, joinedFrom.get(0), aliasToWhere.get(firstTableAlias), aliasToOrderBy.get(firstTableAlias), connectedConditions);
            fillTableSelectedJoin(joinSelect.secondTable, query, joinedFrom.get(1), aliasToWhere.get(secondTableAlias), aliasToOrderBy.get(secondTableAlias), connectedConditions);

            updateJoinLimit(query.getLimit(), joinSelect);

            // todo: throw error feature not supported:  no group bys on joins ?
            return joinSelect;
        }


        Select parseSelect(MySqlSelectQueryBlock query)  {
            Select select = new Select();

            SQLTableSource from = query.getFrom();
            String tblAlias = from == null ? null : from.getAlias();
            if (from != null) {
                List<From> from0 = findFrom(from);
                select.from.addAll(from0);
            }

            List<Field> fields = Lists.transform(query.getSelectList(), it -> Field.make(it.getExpr(), it.getAlias(), tblAlias));
            select.fields.addAll(fields);

            WhereParser whereParser = new WhereParser(this, query);
            select.where = whereParser.parseWhere();
            select.fillSubQueries();
            
            findLimit(query.getLimit(), select);
            findOrderBy(query, select);
            findGroupBy(query, select);

            return select;
        }

        void fillTableSelectedJoin(TableOnJoinSelect tableOnJoin, MySqlSelectQueryBlock query, From tableFrom, ConditionGroup where, List<SQLSelectOrderByItem> orderBys, List<Condition> conditions) {
            String alias = tableFrom.alias;
            fillBasicTableSelectJoin(tableOnJoin, tableFrom, where, orderBys, query);
            tableOnJoin.connectedFields = getConnectedFields(conditions, alias);
            tableOnJoin.selectedFields = new ArrayList<>(tableOnJoin.fields);
            tableOnJoin.alias = alias;
            tableOnJoin.fillSubQueries();
        }

        void fillBasicTableSelectJoin(TableOnJoinSelect select, From from, ConditionGroup where, List<SQLSelectOrderByItem> orderBys, MySqlSelectQueryBlock query) {
            select.from.add(from);
            findSelect(query, select, from.alias);
            select.where = where;
            addOrderByToSelect(select, orderBys, from.alias);
        }

        List<Field> getConnectedFields(List<Condition> conditions, String alias) {
            List<Field> fields = new ArrayList<>();
            String prefix = alias + ".";
            for (Condition condition : conditions) {
                if (condition.name.startsWith(prefix)) {
                    fields.add(new Field(condition.name.replaceFirst(prefix, ""), null));
                } else {
                    if (!((condition.value instanceof SQLPropertyExpr) || (condition.value instanceof SQLIdentifierExpr) || (condition.value instanceof String))) {
                        error("conditions on join should be one side is firstTable second Other , condition was:" + condition.toString());
                    }
                    String aliasDotValue = condition.value.toString();
                    int indexOfDot = aliasDotValue.indexOf(".");
                    String owner = aliasDotValue.substring(0, indexOfDot);
                    if (owner.equals(alias)) {
                        fields.add(new Field(aliasDotValue.substring(indexOfDot + 1), null));
                    }
                }
            }
            return fields;
        }

        void findSelect(MySqlSelectQueryBlock query, Select select, String tableAlias) {
            List<SQLSelectItem> selectList = query.getSelectList();
            for (SQLSelectItem sqlSelectItem : selectList) {
                Field field = Field.make(sqlSelectItem.getExpr(), sqlSelectItem.getAlias(), tableAlias);
                select.addField(field);
            }
        }

        List<Condition> getConditionsFlatten(ConditionGroup where) {
            List<Condition> conditions = new ArrayList<>();
            if (where == null) {
                return conditions;
            }
            addIfConditionRecursive(where, conditions);
            return conditions;
        }

        void addIfConditionRecursive(ConditionGroup where, List<Condition> conditions) {
            if (where instanceof Condition) {
                Condition cond = (Condition) where;
                if (!((cond.value instanceof SQLIdentifierExpr) || (cond.value instanceof SQLPropertyExpr) || (cond.value instanceof String))) {
                    error("conditions on join should be one side is secondTable OPEAR firstTable, condition was:" + cond.toString());
                }
                conditions.add(cond);
            }
            for (ConditionGroup innerWhere : where.wheres) {
                addIfConditionRecursive(innerWhere, conditions);
            }
        }

        void updateJoinLimit(SQLLimit limit, JoinSelect joinSelect) {
            if (limit != null && limit.getRowCount() != null) {
                joinSelect.totalLimit = Integer.parseInt(limit.getRowCount().toString());
            }
        }

        Map<String, List<SQLSelectOrderByItem>> splitAndFindOrder(SQLOrderBy orderBy, String firstTableAlias, String secondTableAlias)  {
            Map<String, List<SQLSelectOrderByItem>> aliasToOrderBys = new HashMap<>();
            aliasToOrderBys.put(firstTableAlias, new ArrayList<SQLSelectOrderByItem>());
            aliasToOrderBys.put(secondTableAlias, new ArrayList<SQLSelectOrderByItem>());
            if (orderBy == null) {
                return aliasToOrderBys;
            }
            List<SQLSelectOrderByItem> orderByItems = orderBy.getItems();
            for (SQLSelectOrderByItem orderByItem : orderByItems) {
                if (orderByItem.getExpr().toString().startsWith(firstTableAlias + ".")) {
                    aliasToOrderBys.get(firstTableAlias).add(orderByItem);
                } else if (orderByItem.getExpr().toString().startsWith(secondTableAlias + ".")) {
                    aliasToOrderBys.get(secondTableAlias).add(orderByItem);
                } else {
                    error("order by field on join request should have alias before, got " + orderByItem.getExpr().toString());
                }
            }
            return aliasToOrderBys;
        }


        JoinSelect createBasicJoinSelectAccordingToTableSource(SQLJoinTableSource joinTableSource) {
            JoinSelect joinSelect = new JoinSelect();
            if (joinTableSource.getCondition() != null) {
                ConditionGroup where = new ConditionGroup();
                WhereParser whereParser = new WhereParser(this, joinTableSource.getCondition());
                whereParser.parseWhere(joinTableSource.getCondition(), joinTableSource.getCondition(), where);
                joinSelect.connectedWhere = where;
            }
            joinSelect.joinType = joinTableSource.getJoinType();
            return joinSelect;
        }

        Map<String, ConditionGroup> splitAndFindWhere(SQLExpr whereExpr, String firstTableAlias, String secondTableAlias) {
            WhereParser whereParser = new WhereParser(this, whereExpr);
            ConditionGroup where = whereParser.parseWhere();
            return splitWheres(where, firstTableAlias, secondTableAlias);
        }

        Map<String, ConditionGroup> splitWheres(ConditionGroup where, String... aliases) {
            Map<String, ConditionGroup> aliasToWhere = new HashMap<>();
            for (String alias : aliases) {
                aliasToWhere.put(alias, null);
            }
            if (where == null) {
                return aliasToWhere;
            }

            String allWhereFromSameAlias = sameAliasWhere(where, aliases);
            if (allWhereFromSameAlias != null) {
                removeAliasPrefix(where, allWhereFromSameAlias);
                aliasToWhere.put(allWhereFromSameAlias, where);
                return aliasToWhere;
            }
            for (ConditionGroup innerWhere : where.wheres) {
                String sameAlias = sameAliasWhere(innerWhere, aliases);
                if (sameAlias == null) {
                    error("Currently support only one hierarchy on different tables where");
                }
                removeAliasPrefix(innerWhere, sameAlias);
                ConditionGroup aliasCurrentWhere = aliasToWhere.get(sameAlias);
                if (aliasCurrentWhere == null) {
                    aliasToWhere.put(sameAlias, innerWhere);
                } else {
                    ConditionGroup andWhereContainer = new ConditionGroup();
                    andWhereContainer.wheres.add(aliasCurrentWhere);
                    andWhereContainer.wheres.add(innerWhere);
                    aliasToWhere.put(sameAlias, andWhereContainer);
                }
            }

            return aliasToWhere;
        }

        void removeAliasPrefix(ConditionGroup where, String alias) {
            if (where instanceof Condition) {
                Condition cond = (Condition) where;
                String aliasPrefix = alias + ".";
                cond.name = cond.name.replaceFirst(aliasPrefix, "");
                return;
            }
            for (ConditionGroup innerWhere : where.wheres) {
                removeAliasPrefix(innerWhere, alias);
            }
        }

        String sameAliasWhere(ConditionGroup where, String... aliases)  {
            if (where == null) {
                return null;
            }

            if (where instanceof Condition) {
                Condition condition = (Condition) where;
                String fieldName = condition.name;
                for (String alias : aliases) {
                    String prefix = alias + ".";
                    if (fieldName.startsWith(prefix)) {
                        return alias;
                    }
                }
                error(String.format("fieldName : %s on codition:%s does not contain alias", fieldName, condition.toString()));
            }
            List<String> sameAliases = new ArrayList<>();
            if (where.wheres != null && where.wheres.size() > 0) {
                for (ConditionGroup innerWhere : where.wheres) {
                    sameAliases.add(sameAliasWhere(innerWhere, aliases));
                }
            }

            if (sameAliases.contains(null)) {
                return null;
            }
            String firstAlias = sameAliases.get(0);
            //return null if more than one alias
            for (String alias : sameAliases) {
                if (!alias.equals(firstAlias)) {
                    return null;
                }
            }
            return firstAlias;
        }

        List<From> findJoinedFrom(SQLTableSource from) {
            SQLJoinTableSource joinTableSource = ((SQLJoinTableSource) from);
            List<From> fromList = new ArrayList<>();
            fromList.addAll(findFrom(joinTableSource.getLeft()));
            fromList.addAll(findFrom(joinTableSource.getRight()));
            return fromList;
        }

        List<From> findFrom(SQLTableSource from) {
            if (from instanceof SQLExprTableSource) {
                SQLExprTableSource fromExpr = (SQLExprTableSource) from;
                return Arrays.stream(fromExpr.getExpr().toString().split(","))
                        .map(it -> new From(it.trim(), fromExpr.getAlias())).collect(toList());
            }

            SQLJoinTableSource joinTableSource = ((SQLJoinTableSource) from);
            List<From> fromList = new ArrayList<>();
            fromList.addAll(findFrom(joinTableSource.getLeft()));
            fromList.addAll(findFrom(joinTableSource.getRight()));
            return fromList;
        }

        void findLimit(SQLLimit limit, Select select) {
            if (limit == null) return;
            select.rowCount = Integer.parseInt(limit.getRowCount().toString());
            if (limit.getOffset() != null)
                select.offset = Integer.parseInt(limit.getOffset().toString());
        }

        void findOrderBy(MySqlSelectQueryBlock query, Select select)  {
            SQLOrderBy orderBy = query.getOrderBy();
            if (orderBy == null) return;
            addOrderByToSelect(select, orderBy.getItems(), null);
        }

        void addOrderByToSelect(Select select, List<SQLSelectOrderByItem> items, String alias)  {
            for (SQLSelectOrderByItem sqlSelectOrderByItem : items) {
                String orderByName = Field.make(sqlSelectOrderByItem.getExpr(), null, null).toString();
                SQLOrderingSpecification type = sqlSelectOrderByItem.getType();
                if (type == null) {
                    type = SQLOrderingSpecification.ASC;
                }
                orderByName = orderByName.replace("`", "");
                if (alias != null) {
                    orderByName = orderByName.replaceFirst(alias + "\\.", "");
                }
                select.addOrderBy(orderByName, type);
            }
        }

        void findGroupBy(MySqlSelectQueryBlock query, Select select)  {
            SQLSelectGroupByClause groupBy = query.getGroupBy();
            if (groupBy == null) return;
            
            // TODO
            // un_support("groupby");

            SQLTableSource sqlTableSource = query.getFrom();
            List<SQLExpr> items = groupBy.getItems();

            List<SQLExpr> standardGroupBys = new ArrayList<>();
            for (SQLExpr sqlExpr : items) {
                if (sqlExpr instanceof MySqlOrderingExpr) {
                    MySqlOrderingExpr sqlSelectGroupByExpr = (MySqlOrderingExpr) sqlExpr;
                    sqlExpr = sqlSelectGroupByExpr.getExpr();
                }

                if ((!(sqlExpr instanceof SQLIdentifierExpr || sqlExpr instanceof SQLMethodInvokeExpr)) &&
                        !standardGroupBys.isEmpty()) {
                    // flush the standard group bys
                    select.addGroupBy(exprsToFields(standardGroupBys, sqlTableSource));
                    standardGroupBys = new ArrayList<>();
                }

                if (sqlExpr instanceof SQLListExpr) {
                    // multiple items in their own list
                    SQLListExpr listExpr = (SQLListExpr) sqlExpr;
                    select.addGroupBy(exprsToFields(listExpr.getItems(), sqlTableSource));
                } else {
                    // everything else gets added to the running list of standard group bys
                    standardGroupBys.add(sqlExpr);
                }
            }
            if (!standardGroupBys.isEmpty()) {
                select.addGroupBy(exprsToFields(standardGroupBys, sqlTableSource));
            }
        }

        List<Field> exprsToFields(List<? extends SQLExpr> exprs, SQLTableSource sqlTableSource)  {
            //here we suppose groupby field will not have alias,so set null in second parameter
            return Lists.transform(exprs, it -> Field.make(it, null, sqlTableSource.getAlias()));
        }
    }






    ////////////////////////////////////////////////////////////////////////////////////////////////

    @SuppressWarnings({"StaticPseudoFunctionalStyleMethod", "Convert2MethodRef"})
    static class ConditionGroup implements Cloneable {
        enum LogicOp {
            AND, OR;
            LogicOp negative() { return this == AND ? OR : AND; }
        }

        List<ConditionGroup> wheres = new ArrayList<>();
        LogicOp logicOp/* = LogicOp.AND*/;

        ConditionGroup() { }
        ConditionGroup(String logic) { logicOp = LogicOp.valueOf(logic.toUpperCase()); }
        ConditionGroup(LogicOp logic) { logicOp = logic; }

        ConditionGroup negate()  {
            if (this instanceof Condition) {
                return negate0(((Condition) this));
            }
            return negate0(this);
        }

        ConditionGroup negate0(ConditionGroup where)  {
            where = ((ConditionGroup) where.clone());
            where.wheres = Lists.transform(where.wheres, it -> {
                ConditionGroup negate = it.negate();
                negate.logicOp = it.logicOp.negative();
                return negate;
            });
            return where;
        }

        Condition negate0(Condition sub)  {
            Condition cond = (Condition) sub.clone();
            cond.op = cond.op.negative();
            return cond;
        }

        @Override
        public Object clone() {
            ConditionGroup cloned = new ConditionGroup(logicOp);
            cloned.wheres = Lists.transform(wheres, it -> ((ConditionGroup) it.clone()));
            return cloned;
        }

        @Override
        public String toString() {
            return String.format("(%s %s)", logicOp,
                    wheres.stream().map(it -> it.toString())
                    .collect(Collectors.joining(" ")));
        }
    }

    static class Condition extends ConditionGroup {

        public enum OP {
            EQ, GT, LT, GTE, LTE, NE, 
            LIKE, NOT_LIKE, 
            IS, IS_NOT, 
            IN, NOT_IN, 
            BETWEEN, NOT_BETWEEN;
            
            // TODO
            // public static Map<String, OP> methodNameToOp;
            // static { }
            static BiMap<OP, OP> negs;
            static {
                negs = HashBiMap.create(7);
                negs.put(EQ, NE);
                negs.put(GT, LTE);
                negs.put(LT, GTE);
                negs.put(LIKE, NOT_LIKE);
                negs.put(IS, IS_NOT);
                negs.put(IN, NOT_IN);
                negs.put(BETWEEN, NOT_BETWEEN);
            }
            
            static OP of(String opStr)  {
                switch (opStr) {
                    case "=": return OP.EQ;
                    case ">": return OP.GT;
                    case "<": return OP.LT;
                    case ">=": return OP.GTE;
                    case "<=": return OP.LTE;
                    case "<>": return OP.NE;
                    case "LIKE": return OP.LIKE;
                    case "NOT": return OP.NE;
                    case "NOT LIKE": return OP.NOT_LIKE;
                    case "IS": return OP.IS;
                    case "IS NOT": return OP.IS_NOT;
                    case "NOT IN": return OP.NOT_IN;
                    case "IN": return OP.IN;
                    case "BETWEEN": return OP.BETWEEN;
                    case "NOT BETWEEN": return OP.NOT_BETWEEN;
                    default: error(opStr + " is err!"); return null;
                }
            }

            OP negative()  {
                OP negative = negs.get(this);
                negative = negative != null ? negative : negs.inverse().get(this);
                if (negative == null) {
                    error("OP negative not supported: " + this);
                }
                return negative;
            }
        }

        OP op;
        String name;
        SQLExpr nameExpr;
        Object value;
        SQLExpr valueExpr;

        Condition(LogicOp logic, 
                  String name, SQLExpr nameExpr, 
                  OP op,
                  Object value, SQLExpr valueExpr) {
            super(logic);
            this.op = op;
            this.name = name;
            this.value = value;
            this.nameExpr = nameExpr;
            this.valueExpr = valueExpr;
        }
        
        Condition(LogicOp logic, 
                  String name, SQLExpr nameExpr, 
                  String opStr,
                  Object value, SQLExpr valueExpr) {
            super(logic);
            this.op = OP.of(opStr);
            this.name = name;
            this.value = value;
            this.nameExpr = nameExpr;
            this.valueExpr = valueExpr;
        }

        @Override
        public Object clone()  { return new Condition(logicOp, name, nameExpr, op, value, valueExpr); }

        @Override
        public String toString() {
            return String.format("(%s %s %s)", name, op, value);
        }
    }

    @AllArgsConstructor
    static class From {
        String table;
        String alias;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @AllArgsConstructor
    static class Field implements Cloneable {
        String name;
        String alias;
        String tableAlias;

        Field(String name, String alias) {
            this.name = name;
            this.alias = alias;
        }

        static Field make(SQLExpr expr, String alias, String tableAlias)  {
            if (expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr || expr instanceof SQLVariantRefExpr) {
                return handleIdentifier(expr, alias, tableAlias);
            } else if (expr instanceof SQLQueryExpr) {
                error("unknown field name : " + expr);
            } else if (expr instanceof SQLBinaryOpExpr) {
                un_support(expr);
            } else if (expr instanceof SQLAllColumnExpr) {
                return handleIdentifier(expr, alias, tableAlias);
            } else if (expr instanceof SQLMethodInvokeExpr) {
                un_support(expr);
            } else if (expr instanceof SQLAggregateExpr) {
                un_support(expr);
            } else if (expr instanceof SQLCaseExpr) {
                un_support(expr);
            } else {
                error("unknown field name : " + expr);
            }
            return null;
        }

        static Field handleIdentifier(SQLExpr expr, String alias, String tableAlias)  {
            if (expr instanceof SQLAllColumnExpr) {
                return new Field("*", alias, tableAlias);
            }

            String name = expr.toString().replace("`", "");
            String newFieldName = name;
            Field field = null;

            if (tableAlias == null) {
                field = new Field(newFieldName, alias, tableAlias);
            } else {
                String aliasPrefix = tableAlias + ".";
                if (name.startsWith(aliasPrefix)) {
                    newFieldName = name.replaceFirst(aliasPrefix, "");
                    field = new Field(newFieldName, alias, tableAlias);
                }
            }

            if (alias != null && !Objects.equals(alias, name) && !isFromJoinOrUnionTable(expr)) {
                un_support(expr);
            }
            return field;
        }

        @Override
        public String toString() {
            return this.name;
        }

        @Override
        public Object clone() { return new Field(name, alias, tableAlias); }
    }

    @AllArgsConstructor
    static class Order {
        public String name;
        public SQLOrderingSpecification type;
    }

    static class SubQueryExpression {
        Object[] values;
        Select select;
        SubQueryExpression(Select innerSelect) {
            select = innerSelect;
            values = null;
        }
    }

    static abstract class Query {
        ConditionGroup where = null;
        List<From> from = new ArrayList<>();
    }

    static class MultiQuerySelect {
        SQLUnionOperator operation;
        Select firstSelect;
        Select secondSelect;

        MultiQuerySelect(SQLUnionOperator operation, Select firstSelect, Select secondSelect) {
            this.operation = operation;
            this.firstSelect = firstSelect;
            this.secondSelect = secondSelect;
        }
    }

    static class TableOnJoinSelect extends Select {
        List<Field> connectedFields;
        List<Field> selectedFields;
        String alias;
    }

    static class JoinSelect {
        TableOnJoinSelect firstTable;
        TableOnJoinSelect secondTable;
        ConditionGroup connectedWhere;
        List<Condition> connectedConditions;
        int totalLimit;
        final int DEAFULT_NUM_OF_RESULTS = 300;
        SQLJoinTableSource.JoinType joinType;


        public JoinSelect() {
            firstTable = new TableOnJoinSelect();
            secondTable = new TableOnJoinSelect();

            totalLimit = DEAFULT_NUM_OF_RESULTS;
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class Select extends Query {
        public List<Field> fields = new ArrayList<>();
        public List<List<Field>> groupBys = new ArrayList<>();
        public List<Order> orderBys = new ArrayList<>();

        public Integer offset;
        public Integer rowCount;

        public boolean containsSubQueries;
        public List<SubQueryExpression> subQueries;
        public boolean selectAll = false;
        public boolean isAgg = false;

        boolean isOrderSelect(){
            return orderBys != null && orderBys.size() > 0 ;
        }

        void addGroupBy(Field field) {
            addGroupBy(Lists.newArrayList(field));
        }

        void addGroupBy(List<Field> fields) {
            isAgg = true;
            groupBys.add(fields);
        }

        void addOrderBy(String name, SQLOrderingSpecification type) {
            orderBys.add(new Order(name, type));
        }

        void addField(Field field) {
            if (field == null ) return;
            if(field.name.equals("*"))
                selectAll = true;
            fields.add(field);
        }

        void fillSubQueries() {
            subQueries = new ArrayList<>();
            fillSubQueriesFromWhereRecursive(where);
        }

        void fillSubQueriesFromWhereRecursive(ConditionGroup where) {
            if(where == null) return;

            if(where instanceof Condition){
                Condition condition = (Condition) where;
                if (condition.value instanceof SubQueryExpression){
                    subQueries.add((SubQueryExpression) condition.value);
                    containsSubQueries = true;
                }
                if(condition.value instanceof List){
                    for(Object o : (List) condition.value){
                        if (o instanceof SubQueryExpression) {
                            subQueries.add((SubQueryExpression) o);
                            containsSubQueries = true;
                        }
                    }
                }
            } else {
                for(ConditionGroup innerWhere : where.wheres) {
                    fillSubQueriesFromWhereRecursive(innerWhere);
                }
            }
        }
    }

    @SuppressWarnings("StaticPseudoFunctionalStyleMethod")
    static class WhereParser {
        SelectParser selectParser;
        MySqlSelectQueryBlock query;
        SQLExpr where;

        WhereParser(SelectParser selectParser, MySqlSelectQueryBlock query) {
            this.selectParser = selectParser;
            this.query = query;
            this.where = query.getWhere();
        }
        WhereParser(SelectParser selectParser, SQLExpr expr) {
            this.selectParser = selectParser;
            this.where = expr;
        }

        public ConditionGroup parseWhere()  {
            if (where == null) return null;
            return parseWhere(where);
        }

        ConditionGroup parseWhere(SQLExpr expr)  {
            ConditionGroup where = new ConditionGroup();
            ConditionGroup parsed = parseWhere(null, expr, where);

            // condition
            if (parsed instanceof Condition) {
                where.wheres.add(parsed);
                if (where.logicOp == null) where.logicOp = AND;
                return where;
            }

            // conditionGroup
            if (parsed != null && where.wheres.isEmpty()) {
                return parsed;
            }

            if (parsed != null) where.wheres.add(parsed);
            if (where.logicOp == null) where.logicOp = AND;
            return where;
        }

        ConditionGroup parseWhere(SQLExpr lastExpr, SQLExpr expr, ConditionGroup parentWhere)  {
            if (expr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr bExpr = (SQLBinaryOpExpr) expr;
                if (explanBothSidesAreLiterals(bExpr, parentWhere)) return null;
                if (explanBothSidesAreProperty(bExpr, parentWhere)) return null;

                if (isCond((SQLBinaryOpExpr) expr)) {
                    Condition cond = explanCond(AND.name(), expr);
                    return cond;
                } else {
                    // 优化：同优先级不生成新的 conditionGroup
                    if (lastExpr instanceof SQLBinaryOpExpr &&
                            bExpr.getOperator().getPriority() != ((SQLBinaryOpExpr) lastExpr).getOperator().priority) {
                        ConditionGroup group = new ConditionGroup(bExpr.getOperator().name);
                        ConditionGroup leftExpr = parseWhere(bExpr, bExpr.getLeft(), group);
                        ConditionGroup rightExpr = parseWhere(bExpr, bExpr.getRight(), group);
                        if (leftExpr != null) group.wheres.add(leftExpr);
                        if (rightExpr!= null) group.wheres.add(rightExpr);
                        return group;
                    } else {
                        parentWhere.logicOp = ConditionGroup.LogicOp.valueOf(bExpr.getOperator().name);
                        ConditionGroup leftExpr = parseWhere(bExpr, bExpr.getLeft(), parentWhere);
                        ConditionGroup rightExpr = parseWhere(bExpr, bExpr.getRight(), parentWhere);
                        if (leftExpr != null) parentWhere.wheres.add(leftExpr);
                        if (rightExpr!= null) parentWhere.wheres.add(rightExpr);
                        return null;
                    }
                }
            } else if (expr instanceof SQLNotExpr) {
                ConditionGroup conditionGroup = parseWhere(lastExpr, ((SQLNotExpr) expr).getExpr(), parentWhere);
                return conditionGroup.negate();
            } else {
                Condition cond = explanCond(AND.name(), expr);
                return cond;
            }
        }


        Condition explanCond(String op, SQLExpr expr)  {
            if (expr instanceof SQLBinaryOpExpr) {
                SQLBinaryOpExpr soExpr = (SQLBinaryOpExpr) expr;
                if (soExpr.getRight() instanceof SQLMethodInvokeExpr) {
                    un_support(expr);
                }
                return new Condition(ConditionGroup.LogicOp.valueOf(op), soExpr.getLeft().toString(), soExpr.getLeft(), soExpr.getOperator().name,
                        parseValue(soExpr.getRight()), soExpr.getRight());
            } else if (expr instanceof SQLInListExpr) {
                SQLInListExpr siExpr = (SQLInListExpr) expr;
                String leftSide = siExpr.getExpr().toString();
                return new Condition(ConditionGroup.LogicOp.valueOf(op), leftSide, null, siExpr.isNot() ? "NOT IN" : "IN",
                        parseValue(siExpr.getTargetList()), null);
            } else if (expr instanceof SQLBetweenExpr) {
                SQLBetweenExpr between = ((SQLBetweenExpr) expr);
                String leftSide = between.getTestExpr().toString();
                return new Condition(ConditionGroup.LogicOp.valueOf(op), leftSide, null, between.isNot() ? "NOT BETWEEN" : "BETWEEN",
                        Lists.newArrayList(parseValue(between.beginExpr), parseValue(between.endExpr)), null);
            } else if (expr instanceof SQLMethodInvokeExpr) {
                un_support(expr);
            } else if (expr instanceof SQLInSubQueryExpr) {
                un_support(expr);

                SQLInSubQueryExpr sqlIn = (SQLInSubQueryExpr) expr;
                Select innerSelect = selectParser.parseSelect((MySqlSelectQueryBlock) sqlIn.getSubQuery().getQuery());
                SubQueryExpression subQueryExpression = new SubQueryExpression(innerSelect);
                String leftSide = sqlIn.getExpr().toString();
                return new Condition(ConditionGroup.LogicOp.valueOf(op), leftSide, null, sqlIn.isNot() ? "NOT IN" : "IN", subQueryExpression, null);
            } else {
                error("err find condition " + expr.getClass());
            }
            return null;
        }

        List<String> parseValue(List<SQLExpr> targetList)  {
            return Lists.transform(targetList, it -> parseValue(it));
        }

        String parseValue(SQLExpr expr)  {
            if (expr instanceof SQLNumericLiteralExpr) {
                return ((SQLNumericLiteralExpr) expr).getNumber() + "";
            } else if (expr instanceof SQLCharExpr) {
                return ((SQLCharExpr) expr).getText();
            } else if (expr instanceof SQLMethodInvokeExpr) {
                un_support(expr);
                // return expr;
                return null;
            } else if (expr instanceof SQLNullExpr) {
                return null;
            } else if (expr instanceof SQLIdentifierExpr) {
                un_support(expr);
                // return expr;
                return null;
            } else if (expr instanceof SQLPropertyExpr) {
                un_support(expr);
                // return expr;
                return null;
            } else if (expr instanceof SQLBooleanExpr) {
                return ((SQLBooleanExpr) expr).getValue().toString();
            } else  {
                error(String.format("Failed to parse SqlExpression of type %s. expression value: %s", expr.getClass(), expr));
                return null;
            }
        }

        //some where conditions eg. 1=1 or 3>2 or 'a'='b'
        boolean explanBothSidesAreLiterals(SQLBinaryOpExpr bExpr, ConditionGroup where) {
            if ((bExpr.getLeft() instanceof SQLNumericLiteralExpr || bExpr.getLeft() instanceof SQLCharExpr) &&
                    (bExpr.getRight() instanceof SQLNumericLiteralExpr || bExpr.getRight() instanceof SQLCharExpr)
            ) {
                un_support(bExpr);
                // 常量表达式直接执行... SQLEvalVisitorUtils.eval(bExpr.getDbType(), bExpr);
                // explanCond("AND", ???, where);
                return true;
            }
            return false;
        }

        //some where conditions eg. field1=field2 or field1>field2
        boolean explanBothSidesAreProperty(SQLBinaryOpExpr bExpr, ConditionGroup where) {
            //join is not support
            if ((bExpr.getLeft() instanceof SQLPropertyExpr || bExpr.getLeft() instanceof SQLIdentifierExpr) &&
                    (bExpr.getRight() instanceof SQLPropertyExpr || bExpr.getRight() instanceof SQLIdentifierExpr) &&
                    Sets.newHashSet("=", "<", ">", ">=", "<=").contains(bExpr.getOperator().getName()) &&
                    !isFromJoinOrUnionTable(bExpr)

            ) {
                un_support(bExpr);

                String leftProperty = expr2Object(bExpr.getLeft()).toString();
                String rightProperty = expr2Object(bExpr.getRight()).toString();
                // explanCond("AND", sqlMethodInvokeExpr, where);
                return true;
            }
            return false;
        }
    }
}
