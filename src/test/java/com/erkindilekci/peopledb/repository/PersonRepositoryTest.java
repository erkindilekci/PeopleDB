package com.erkindilekci.peopledb.repository;

import com.erkindilekci.peopledb.model.Address;
import com.erkindilekci.peopledb.model.Person;
import com.erkindilekci.peopledb.model.Region;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class PersonRepositoryTest {

    private Connection connection;
    private PersonRepository repo;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager.getConnection("jdbc:h2:~/peopletest".replace("~", System.getProperty("user.home")));
        connection.setAutoCommit(false);
        repo = new PersonRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        ZonedDateTime timestampOfJohn = ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"));
        Person john = new Person("test", "test", timestampOfJohn);
        Person savedPerson = repo.save(john);

        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        ZonedDateTime timestampOfJohn = ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"));
        Person john = new Person("test1", "test1", timestampOfJohn);
        Person savedPerson1 = repo.save(john);

        ZonedDateTime timestampOfBobby = ZonedDateTime.of(1990, 1, 12, 12, 13, 3, 0, ZoneId.of("-6"));
        Person bobby = new Person("test2", "test2", timestampOfBobby);
        Person savedPerson2 = repo.save(bobby);

        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }

    @Test
    public void canSavePersonWithHomeAddress() {
        ZonedDateTime timestampOfJohn = ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"));
        Person john = new Person("test1", "test1", timestampOfJohn);

        Address address = new Address(null, "123 Cookie St", "Apt. 9B", "Washington", "WA", "30340", "United States", "Fulton County", Region.WEST);

        john.setHomeAddress(address);

        Person savedPerson = repo.save(john);

        assertThat(savedPerson.getHomeAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithBusinessAddress() {
        ZonedDateTime timestampOfJohn = ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"));
        Person john = new Person("test1", "test1", timestampOfJohn);

        Address address = new Address(null, "123 Cookie St", "Apt. 9B", "Washington", "WA", "30340", "United States", "Fulton County", Region.WEST);

        john.setBusinessAddress(address);

        Person savedPerson = repo.save(john);

        assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithChildren() {
        ZonedDateTime timestampOfJohn = ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"));
        Person john = new Person("test1", "test1", timestampOfJohn);
        john.addChild(new Person("childtest1", "childtest", timestampOfJohn.minusYears(24)));
        john.addChild(new Person("childtest2", "childtest2", timestampOfJohn.minusYears(26)));
        john.addChild(new Person("childtest3", "childtest3", timestampOfJohn.minusYears(28)));

        Person savedPerson = repo.save(john);

        savedPerson.getChildren().stream()
                .map(Person::getId)
                .forEach(id -> assertThat(id).isGreaterThan(0));
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = repo.save(new Person("test", "test", ZonedDateTime.now()));
        Person foundPerson = repo.findById(savedPerson.getId()).get();

        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindPersonByIdWithHomeAddress() {
        ZonedDateTime timestampOfJohn = ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"));
        Person john = new Person("test1", "test1", timestampOfJohn);
        Address address = new Address(null, "123 Cookie St", "Apt. 9B", "Washington", "WA", "30340", "United States", "Fulton County", Region.WEST);
        john.setHomeAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId().longValue()).get();

        assertThat(foundPerson.getHomeAddress().get().state()).isEqualTo("WA");
    }

    @Test
    public void canFindPersonByIdWithBusinessAddress() {
        ZonedDateTime timestampOfJohn = ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"));
        Person john = new Person("test1", "test1", timestampOfJohn);
        Address address = new Address(null, "123 Cookie St", "Apt. 9B", "Washington", "WA", "30340", "United States", "Fulton County", Region.WEST);
        john.setBusinessAddress(address);

        Person savedPerson = repo.save(john);
        Person foundPerson = repo.findById(savedPerson.getId().longValue()).get();

        assertThat(foundPerson.getBusinessAddress().get().state()).isEqualTo("WA");
    }

    @Test
    public void canFindPersonWithChildren() {
        ZonedDateTime timestampOfJohn = ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"));
        Person john = new Person("test1", "test1", timestampOfJohn);
        john.addChild(new Person("childtest1", "childtest1", timestampOfJohn.minusYears(24)));
        john.addChild(new Person("childtest2", "childtest2", timestampOfJohn.minusYears(26)));
        john.addChild(new Person("childtest3", "childtest3", timestampOfJohn.minusYears(28)));

        Person savedPerson = repo.save(john);

        Person foundPerson = repo.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getChildren().stream().map(Person::getFirstName).collect(Collectors.toSet()))
                .contains("childtest1", "childtest2", "childtest3");
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = repo.findById(-1L);
        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canFindAll() {
        repo.save(new Person("test1", "test1", ZonedDateTime.now()));
        repo.save(new Person("test2", "test2", ZonedDateTime.now()));
        repo.save(new Person("test3", "test3", ZonedDateTime.now()));
        repo.save(new Person("test4", "test4", ZonedDateTime.now()));
        repo.save(new Person("test5", "test5", ZonedDateTime.now()));
        repo.save(new Person("test6", "test6", ZonedDateTime.now()));
        repo.save(new Person("test7", "test7", ZonedDateTime.now()));
        repo.save(new Person("test8", "test8", ZonedDateTime.now()));
        repo.save(new Person("test9", "test9", ZonedDateTime.now()));
        repo.save(new Person("test10", "test10", ZonedDateTime.now()));

        List<Person> people = repo.findAll();

        assertThat(people.size()).isGreaterThanOrEqualTo(10);
    }

    @Test
    public void canGetCount() {
        long startCount = repo.count();
        repo.save(new Person("test1", "test1", ZonedDateTime.now()));
        repo.save(new Person("test2", "test2", ZonedDateTime.now()));
        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDeleteOnePerson() {
        Person savedPerson = repo.save(new Person("test", "test", ZonedDateTime.now()));
        long startCount = repo.count();

        repo.delete(savedPerson);
        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person p1 = repo.save(new Person("test1", "test1", ZonedDateTime.now()));
        Person p2 = repo.save(new Person("test2", "test2", ZonedDateTime.now()));
        long startCount = repo.count();

        repo.delete(p1, p2);
        long endCount = repo.count();

        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void canUpdate() {
        Person savedPerson = repo.save(new Person("test", "test", ZonedDateTime.now()));
        Person p1 = repo.findById(savedPerson.getId()).get();

        savedPerson.setSalary(new BigDecimal("23000.99"));

        repo.update(savedPerson);
        Person p2 = repo.findById(savedPerson.getId()).get();

        assertThat(p2.getSalary()).isNotEqualTo(p1.getSalary());
    }

    @Test
    @Disabled
    public void loadData() throws IOException {
        Files.lines(Path.of("C:\\Users\\Erkin\\Downloads\\people.csv"))
                .skip(1)
                .map(l -> l.split(","))
                .map(a -> {
                    LocalDate dob = LocalDate.parse(a[10], DateTimeFormatter.ofPattern("M/d/yyyy"));
                    LocalTime tob = LocalTime.parse(a[11], DateTimeFormatter.ofPattern("hh:mm:ss a", Locale.US));
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
                    Person person = new Person(a[2], a[4], zdtob);
                    person.setSalary(new BigDecimal(a[25]));
                    person.setEmail(a[6]);
                    return person;
                })
                .forEach(repo::save);
    }


}
