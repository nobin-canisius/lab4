package edu.canisius.cyb600.lab4.database;

import edu.canisius.cyb600.lab4.dataobjects.Actor;
import edu.canisius.cyb600.lab4.dataobjects.Category;
import edu.canisius.cyb600.lab4.dataobjects.Film;

import java.sql.Connection;
import java.util.List;

/**
 * Abstract DB Adapter
 */
public abstract class AbstractDBAdapter {
    Connection conn;

    public AbstractDBAdapter(Connection conn) {
        this.conn = conn;
    }

    //SELECTS
    public abstract List<String> getAllDistinctCategoryNames();

    public abstract List<Film> getAllFilmsWithALengthLongerThanX(int length);

    public abstract List<Actor> getActorsFirstNameStartingWithX(char firstLetter);
    //INSERTS
    public abstract List<Actor> testInsertAllActorsWithAnOddNumberLastName(List<Actor> actors);
    //JOIN
    public abstract List<Film> getFilmsInCategory(Category category);

}
