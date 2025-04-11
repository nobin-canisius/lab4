package edu.canisius.cyb600.in_class_assignment.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }

    @Override
    public List<Actor> getAllActors(){
        try (Statement statement = conn.createStatement()) {
            //This statement is easy
            //Select * from actor is saying "Return all Fields for all rows in films". Because there
            //is no "where clause", all rows are returned
            ResultSet results = statement.executeQuery("Select * from actor");

            //Initialize an empty List to hold the return set of films.
            List<Actor> actors = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Actor actor = new Actor();
                actor.setActorId(results.getInt("ACTOR_ID"));
                actor.setFirstName(results.getString("first_name"));
                actor.setLastName(results.getString("last_name"));
                actors.add(actor);
            }
            //Return all the films.
            return actors;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }
    @Override
    public List<Actor> getAllActorsWithLastNameWithCode(String lastName) {
        List<Actor> allActors = getAllActors();
        return allActors.stream()
                .filter(actor -> actor.getLastName().equalsIgnoreCase(lastName))
                .collect(Collectors.toList());
    }
    @Override
    public List<Film> getAllFilms() {
        //First, we are going to open up a new statement
        try (Statement statement = conn.createStatement()) {
            //This statement is easy
            //Select * from actor is saying "Return all Fields for all rows in films". Because there
            //is no "where clause", all rows are returned
            ResultSet results = statement.executeQuery("Select * from film");
            //Initialize an empty List to hold the return set of films.
            List<Film> films = new ArrayList<>();
            //Loop through all the results and create a new Film object to hold all its information
            while (results.next()) {
                Film film = new Film();
                film.setFilmId(results.getInt("FILM_ID"));
                film.setTitle(results.getString("TITLE"));
                film.setDescription(results.getString("DESCRIPTION"));
                film.setReleaseYear(results.getString("RELEASE_YEAR"));
                film.setLanguageId(results.getInt("LANGUAGE_ID"));
                film.setRentalDuration(results.getInt("RENTAL_DURATION"));
                film.setRentalRate(results.getDouble("RENTAL_RATE"));
                film.setLength(results.getInt("LENGTH"));
                film.setReplacementCost(results.getDouble("REPLACEMENT_COST"));
                film.setRating(results.getString("RATING"));
                film.setSpecialFeatures(results.getString("SPECIAL_FEATURES"));
                film.setLastUpdate(results.getDate("LAST_UPDATE"));
                //Add film to the array
                films.add(film);
            }
            //Return all the films.
            return films;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public List<Actor> getActorsWithLastName(String lastName) {
        List<Actor> actors = new ArrayList<>();
        String query = "SELECT * FROM actor WHERE LOWER(last_name) = LOWER(?)";

        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setString(1, lastName);
            ResultSet results = statement.executeQuery();

            while (results.next()) {
                Actor actor = new Actor();
                actor.setActorId(results.getInt("actor_id"));
                actor.setFirstName(results.getString("first_name"));
                actor.setLastName(results.getString("last_name"));
                actor.setLastUpdate(results.getTimestamp("last_update"));
                actors.add(actor);
            }
        } catch (SQLException exception) {
            exception.printStackTrace();
        }

        return actors;
    }

    @Override
    public Actor addActor(Actor actor) {

        String sql = "INSERT INTO ACTOR (first_name, last_name) VALUES (? , ? ) returning ACTOR_ID, LAST_UPDATE";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            int i = 1;
            statement.setString(i++, actor.getFirstName().toUpperCase());
            statement.setString(i++, actor.getLastName().toUpperCase());
            ResultSet results = statement.executeQuery();
            if (results.next()) {
                actor.setActorId(results.getInt("ACTOR_ID"));
                actor.setLastUpdate(results.getDate("LAST_UPDATE"));
            }
            return actor;
        } catch (SQLException exception) {
            exception.printStackTrace();
        }
        return actor;

    }

    @Override
    public List<Film> getFilmsForActor(Actor actor) {
        List<Film> films = new ArrayList<>();
        String query = """
        SELECT f.*
        FROM film f
        JOIN film_actor fa ON f.film_id = fa.film_id
        WHERE fa.actor_id = ?
    """;

        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setInt(1, actor.getActorId());
            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
                Film film = new Film();
                film.setFilmId(rs.getInt("film_id"));
                film.setTitle(rs.getString("title"));
                film.setDescription(rs.getString("description"));
                film.setReleaseYear(rs.getString("release_year"));
                film.setLanguageId(rs.getInt("language_id"));
                film.setRentalDuration(rs.getInt("rental_duration"));
                film.setRentalRate(rs.getDouble("rental_rate"));
                film.setLength(rs.getInt("length"));
                film.setReplacementCost(rs.getDouble("replacement_cost"));
                film.setRating(rs.getString("rating"));
                film.setSpecialFeatures(rs.getString("special_features"));
                film.setLastUpdate(rs.getDate("last_update"));
                films.add(film);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return films;
    }
}
