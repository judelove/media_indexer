import java.sql.*;

public class DBAgent {
    String url;

    DBAgent(String url)
    {
        this.url = url;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url);
    }

    /**
     * Executes an insert operation basing on the supplied query
     * @param query String query used to insert records
     * @return number of rows affected by the insert operation
     * @throws SQLException in the event of a DB failure
     */
    public int bulkInsert(String query) throws SQLException {
        try(Connection con = getConnection()) {
            Statement statement = con.createStatement();
            return statement.executeUpdate(query);
        }
    }
}
