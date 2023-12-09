package com.erkindilekci.peopledb.annotation;

import com.erkindilekci.peopledb.model.CrudOperation;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
@Repeatable(MultiSQL.class)
public @interface SQL {

    String value();
    CrudOperation operationType();
}
