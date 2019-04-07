/**
 * Copyright 2012-2015 Niall Gallagher
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.googlecode.cqengine.query.parser.sql2;

import com.alibaba.druid.sql.ast.SQLOrderingSpecification;
import com.google.common.collect.Lists;
import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.query.Query;
import com.googlecode.cqengine.query.QueryFactory;
import com.googlecode.cqengine.query.logical.And;
import com.googlecode.cqengine.query.logical.Or;
import com.googlecode.cqengine.query.option.AttributeOrder;
import com.googlecode.cqengine.query.option.OrderByOption;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.query.parser.common.InvalidQueryException;
import com.googlecode.cqengine.query.parser.common.ParseResult;
import com.googlecode.cqengine.query.parser.common.QueryParser;
import com.googlecode.cqengine.query.parser.sql2.support.FallbackValueParser;
import com.googlecode.cqengine.query.parser.sql2.support.StringParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.googlecode.cqengine.query.QueryFactory.*;

/**
 * A parser for SQL queries.
 *
 * @author Niall Gallagher
 */
@SuppressWarnings("ALL")
public class SQLParser<O> extends QueryParser<O> {

    SQLPreParser  sqlPreParser = new SQLPreParser();

    public SQLParser(Class<O> objectType) {
        super(objectType);
        StringParser stringParser = new StringParser();
        super.registerValueParser(String.class, stringParser);
        super.registerFallbackValueParser(new FallbackValueParser(stringParser));
    }

    @Override
    public ParseResult<O> parse(String query) {
        try {
            if (query == null) {
                throw new IllegalArgumentException("Query was null");
            }
            SQLPreParser.Select select = sqlPreParser.parseSimpleSelect(query);
            return new ParseResult<>(getParsedQuery(select), getQueryOptions(select));
        }
        catch (InvalidQueryException e) {
            throw e;
        }
        catch (Exception e) {
            throw new InvalidQueryException("Failed to parse query", e);
        }
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////

    // TODO 子查询....
//    protected final Map<ParserRuleContext, Collection<Query<O>>> childQueries = new HashMap<>();


    Query<O> getParsedQuery(SQLPreParser.Select select) {
        if (select.where == null) {
            return QueryFactory.all(this.getObjectType());
        }
        return convertConditionGroup(select.where);

//        Collection<Query<O>> rootQuery = childQueries.get(null);
//        // TODO: 支持子查询 !!!
//        validateExpectedNumberOfChildQueries(1, rootQuery.size());
    }


    Query<O> convertConditionGroup(SQLPreParser.ConditionGroup group) {
        List<Query<O>> qrys = Lists.transform(group.wheres, it ->
                it instanceof SQLPreParser.Condition ?
                        convertCondition(((SQLPreParser.Condition) it)) :
                        convertConditionGroup(it));

        if (group.logicOp == SQLPreParser.ConditionGroup.LogicOp.AND) {
            if (qrys.size() == 1) return qrys.get(0);
            return new And<O>(qrys);
        } else if (group.logicOp == SQLPreParser.ConditionGroup.LogicOp.OR) {
            return new Or<O>(qrys);
        } else {
            throw new IllegalStateException();
        }
        // QueryFactory.not()
    }

    Query<O> convertCondition(SQLPreParser.Condition cond) {
        switch (cond.op) {
            case EQ:
                Attribute<O, Object> attribute_eq = getAttribute(cond.name, Object.class);
                Object value_eq = parseValue(attribute_eq.getAttributeType(), ((String) cond.value));
                return equal(attribute_eq, value_eq);
            case NE:
                Attribute<O, Object> attribute_ne = getAttribute(cond.name, Object.class);
                Object value_ne = parseValue(attribute_ne.getAttributeType(), ((String) cond.value));
                return not(equal(attribute_ne, value_ne));
            case LTE:
                Attribute<O, Comparable> attribute_lte = getAttribute(cond.name, Comparable.class);
                Comparable value_lte = parseValue(attribute_lte.getAttributeType(), ((String) cond.value));
                return lessThanOrEqualTo(attribute_lte, value_lte);
            case LT:
                Attribute<O, Comparable> attribute_lt = getAttribute(cond.name, Comparable.class);
                Comparable value_lt = parseValue(attribute_lt.getAttributeType(), ((String) cond.value));
                return lessThan(attribute_lt, value_lt);
            case GTE:
                Attribute<O, Comparable> attribute_gte = getAttribute(cond.name, Comparable.class);
                Comparable value_gte = parseValue(attribute_gte.getAttributeType(), ((String) cond.value));
                return greaterThanOrEqualTo(attribute_gte, value_gte);
            case GT:
                Attribute<O, Comparable> attribute_gt = getAttribute(cond.name, Comparable.class);
                Comparable value_gt = parseValue(attribute_gt.getAttributeType(), ((String) cond.value));
                return greaterThan(attribute_gt, value_gt);
            case IN:
                Attribute<O, Object> attribute_in = getAttribute(cond.name, Object.class);
                List<Object> values_in = Lists.transform(((List<String>) cond.value), it ->
                        parseValue(attribute_in.getAttributeType(), it));
                return in(attribute_in, values_in);
            case NOT_IN:
                Attribute<O, Object> attribute_not_in = getAttribute(cond.name, Object.class);
                List<Object> values_not_in = Lists.transform(((List<String>) cond.value), it ->
                        parseValue(attribute_not_in.getAttributeType(), it));
                return not(in(attribute_not_in, values_not_in));
            case BETWEEN:
                Attribute<O, Comparable> attribute_between = getAttribute(cond.name, Comparable.class);
                List<String> values_between = ((List<String >) cond.value);
                Comparable lowerValue_between = parseValue(attribute_between.getAttributeType(), values_between.get(0));
                Comparable upperValue_between = parseValue(attribute_between.getAttributeType(), values_between.get(1));
                return between(attribute_between, lowerValue_between, upperValue_between);
            case NOT_BETWEEN:
                Attribute<O, Comparable> attribute_not_between = getAttribute(cond.name, Comparable.class);
                List<String> values_not_between = ((List<String >) cond.value);
                Comparable lowerValue_not_between = parseValue(attribute_not_between.getAttributeType(), values_not_between.get(0));
                Comparable upperValue_not_between = parseValue(attribute_not_between.getAttributeType(), values_not_between.get(1));
                return not(between(attribute_not_between,
                        lowerValue_not_between, upperValue_not_between));
            case LIKE:
                return getLikeQuery(cond);
            case NOT_LIKE:
                return not(getLikeQuery(cond));
            case IS:
                Attribute<O, Object> attribute_is = getAttribute(cond.name, Object.class);
                return not(has(attribute_is));
            case IS_NOT:
                Attribute<O, Object> attribute_is_not = getAttribute(cond.name, Object.class);
                return has(attribute_is_not);
            default:
                throw new IllegalStateException();
        }
    }


    Query<O> getLikeQuery(SQLPreParser.Condition cond) {
        Attribute<O, String> attribute_like = getAttribute(cond.name, String.class);
        String value_like = parseValue(attribute_like.getAttributeType(), ((String) cond.value));
        boolean leadingPercent = value_like.startsWith("%");
        boolean trailingPercent = value_like.endsWith("%");
        if (leadingPercent && trailingPercent) {
            value_like = value_like.substring(1, value_like.length() - 1);
            return contains(attribute_like, value_like);
        } else if (leadingPercent) {
            value_like = value_like.substring(1, value_like.length());
            return endsWith(attribute_like, value_like);
        } else if (trailingPercent) {
            value_like = value_like.substring(0, value_like.length() - 1);
            return startsWith(attribute_like, value_like);
        } else {
            return equal(attribute_like, value_like);
        }
    }

    QueryOptions getQueryOptions(SQLPreParser.Select select) {
        if (select.orderBys == null || select.orderBys.isEmpty()) {
            return noQueryOptions();
        } else {
            List<AttributeOrder<O>> attributeOrders = new ArrayList<>();
            for (SQLPreParser.Order orderBy : select.orderBys) {
                Attribute<O, Comparable> attribute = this.getAttribute(orderBy.name, Comparable.class);
                attributeOrders.add(new AttributeOrder<>(attribute, SQLOrderingSpecification.DESC.equals(orderBy.type)));
            }
            OrderByOption<O> orderByOption = orderBy(attributeOrders);
            return QueryFactory.queryOptions(orderByOption);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////




    /**
     * Creates a new SQLParser for the given POJO class.
     * @param pojoClass The type of object stored in the collection
     * @return a new SQLParser for the given POJO class
     */
    public static <O> SQLParser<O> forPojo(Class<O> pojoClass) {
        return new SQLParser<O>(pojoClass);
    }

    /**
     * Creates a new SQLParser for the given POJO class, and registers the given attributes with it.
     * @param pojoClass The type of object stored in the collection
     * @param attributes The attributes to register with the parser
     * @return a new SQLParser for the given POJO class
     */
    public static <O> SQLParser<O> forPojoWithAttributes(Class<O> pojoClass, Map<String, ? extends Attribute<O, ?>> attributes) {
        SQLParser<O> parser = forPojo(pojoClass);
        parser.registerAttributes(attributes);
        return parser;
    }
}
