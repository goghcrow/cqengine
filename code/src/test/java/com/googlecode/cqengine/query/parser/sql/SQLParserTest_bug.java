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
package com.googlecode.cqengine.query.parser.sql;

import com.googlecode.cqengine.attribute.Attribute;
import com.googlecode.cqengine.attribute.SimpleAttribute;
import com.googlecode.cqengine.query.option.QueryOptions;
import com.googlecode.cqengine.testutil.Car;
import org.junit.Test;

/**
 * @author chuxiaofeng
 */
public class SQLParserTest_bug {

    static final Attribute<Car, Boolean> IS_BLUE = new SimpleAttribute<Car, Boolean>("is_blue") {
        @Override
        public Boolean getValue(Car object, QueryOptions queryOptions) {
            return object.getColor().equals(Car.Color.BLUE);
        }
    };

    // final SQLParser<Car> parser = SQLParser.forPojoWithAttributes(Car.class, createAttributes(Car.class));
    final SQLParser<Car> parser = new SQLParser<Car>(Car.class){{
        registerAttribute(Car.CAR_ID);
        registerAttribute(Car.MANUFACTURER);
        registerAttribute(Car.MODEL);
        registerAttribute(Car.COLOR);
        registerAttribute(Car.DOORS);
        registerAttribute(Car.PRICE);
        registerAttribute(Car.FEATURES);
        registerAttribute(IS_BLUE);
    }};


    @Test
    public void test_括号bug() {
        String sql = "select * from cars where ((   ( carId IN (1,2,3,4) )   AND (price = -3.14)   ))";
        parser.query(sql);
    }
}