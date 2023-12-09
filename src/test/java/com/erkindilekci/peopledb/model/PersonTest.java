package com.erkindilekci.peopledb.model;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class PersonTest {

    @Test
    public void testForEquality() {
        Person p1 = new Person("p1", "smith", ZonedDateTime.of(1990, 2, 4, 1, 30, 20, 0, ZoneId.of("+3")));
        Person p2 = new Person("p1", "smith", ZonedDateTime.of(1990, 2, 4, 1, 30, 20, 0, ZoneId.of("+3")));

        assertThat(p1).isEqualTo(p2);
    }

    @Test
    public void testForInequality() {
        Person p1 = new Person("p1", "smith", ZonedDateTime.of(1990, 2, 4, 1, 30, 20, 0, ZoneId.of("+3")));
        Person p2 = new Person("p2", "smith", ZonedDateTime.of(1990, 2, 4, 1, 30, 20, 0, ZoneId.of("+3")));

        assertThat(p1).isNotEqualTo(p2);
    }
}
