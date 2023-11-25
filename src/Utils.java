import java.sql.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Utils {
    public static String TABLE_NAME = "dre";

    public static final int BATCH_SIZE = 500;

    public static long RUN_FAILED = -1;

    public static String getRandomString(int targetStringLength) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();
    }

    public static void createTable(Connection connection) {
        try {
            String createTableSQL = "CREATE TABLE " + TABLE_NAME + " (nums NUMBER(10) PRIMARY KEY, txt VARCHAR2(255))";
            Statement statement = connection.createStatement();
            statement.executeUpdate(createTableSQL);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void generateNewRowsBatch(Connection connection, int count) throws SQLException {
        String insertRowSQL = "INSERT INTO " + TABLE_NAME + " (txt, nums) VALUES (?, ?)";
        long startTime = System.nanoTime();
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertRowSQL)) {
            Random random = new Random();
            IntStream.range(0, count).forEach(i -> {
                try {
                    preparedStatement.setString(1, getRandomString(255));
                    preparedStatement.setInt(2, random.nextInt(Integer.MAX_VALUE));
                    preparedStatement.addBatch();

                    // Execute the batch periodically (batch size up to 1000)
                    if (i % BATCH_SIZE == 0) {
                        preparedStatement.executeBatch();
                        System.out.println("Inserted " + i);
                    }
                } catch (SQLException e) {
                    // will complain about constraint violation
                }
            });

            // Execute any remaining statements in the batch
            try {
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                // will complain about constraint violation
            }
            long endTime = System.nanoTime();
            System.out.println("Inserting " + count + " took " + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms");
        }
    }

    public static ResultSet oneScan(Connection connection, String query) throws SQLException {
        Statement statement = connection.createStatement();
        return statement.executeQuery(query);
    }

    public static int getRowCount(Connection connection) throws SQLException {
        ResultSet cursor = oneScan(connection, "select count(1) from " + TABLE_NAME);
        cursor.next();
        return cursor.getInt(1);
    }

    public static Stream<String> createRowNumRanges(int rangesCount, int rowCount) {
        int bucketSize = Math.floorDiv(rowCount, rangesCount);
        return IntStream.rangeClosed(0, rangesCount).mapToObj((i) -> {
            int lowerBucket = i * bucketSize;
            int upperBucket = (i + 1) * bucketSize;
            return "ROWNUM > " + lowerBucket + " AND ROWNUM <= " + Math.min(upperBucket, rowCount);
        });
    }
}
