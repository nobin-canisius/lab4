package edu.canisius.cyb600.lab4.database;
import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Posgres Implementation of the db adapter.
 */
public class PostgresDBAdapter extends AbstractDBAdapter {

    public PostgresDBAdapter(Connection conn) {
        super(conn);
    }
    @Override
    public List<String> getAllDistinctCategoryNames() {
        List<String> categories = new ArrayList<>();
        String query = "SELECT DISTINCT name FROM category";

        try (Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                categories.add(rs.getString("name"));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return categories;
    }
    @Override
    public List<Film> getAllFilmsWithALengthLongerThanX(int length) {
        List<Film> films = new ArrayList<>();
        String query = "SELECT * FROM film WHERE length > ?";

        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setInt(1, length);
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
    @Override
    public List<Actor> getActorsFirstNameStartingWithX(char firstLetter) {
        List<Actor> actors = new ArrayList<>();
        String query = "SELECT * FROM actor WHERE first_name ILIKE ?";

        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setString(1, firstLetter + "%");
            ResultSet rs = statement.executeQuery();

            while (rs.next()) {
                Actor actor = new Actor();
                actor.setActorId(rs.getInt("actor_id"));
                actor.setFirstName(rs.getString("first_name"));
                actor.setLastName(rs.getString("last_name"));
                actor.setLastUpdate(rs.getTimestamp("last_update"));
                actors.add(actor);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return actors;
    }

    @Override
    public List<Actor> testInsertAllActorsWithAnOddNumberLastName(List<Actor> actors) {
        List<Actor> insertedActors = new ArrayList<>();
        String sql = "INSERT INTO actor (first_name, last_name) VALUES (?, ?) RETURNING actor_id, last_update";

        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            for (Actor actor : actors) {
                if (actor.getLastName().length() % 2 == 0) { // even-length last name
                    statement.setString(1, actor.getFirstName());
                    statement.setString(2, actor.getLastName());

                    ResultSet rs = statement.executeQuery();
                    if (rs.next()) {
                        actor.setActorId(rs.getInt("actor_id"));
                        actor.setLastUpdate(rs.getTimestamp("last_update"));
                        insertedActors.add(actor);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return insertedActors;
    }

    @Override
    public List<Film> getFilmsInCategory(Category category) {
        List<Film> films = new ArrayList<>();
        String query = """
        SELECT f.*
        FROM film f
        JOIN film_category fc ON f.film_id = fc.film_id
        WHERE fc.category_id = ?
    """;

        try (PreparedStatement statement = conn.prepareStatement(query)) {
            statement.setInt(1, category.getCategoryId());
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
