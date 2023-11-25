import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.averagingLong;
import static java.util.stream.Collectors.joining;

public class Main {
    // see https://bitquill.atlassian.net/wiki/spaces/DKB/pages/2901475361/Run+Oracle+Database+in+Docker+on+ARM64+like+MacBook+M1
    // for Oracle in Docker on Mac setup
    private static final String url = "jdbc:oracle:thin:@localhost:1521:ORCLCDB";
    private static final String user = "system";
    private static final String password = "welcome123";
    private static Connection connection = null;

    private static void oneScanShallow(Connection connection, String query, boolean isVerbose) {
        if (isVerbose) {
            System.out.println("Started " + query);
        }
        try {
            Statement statement = connection.createStatement();
            long startTime = System.nanoTime();
            try (ResultSet resultSet = statement.executeQuery(query)) {
                int counter = 0;
                while (resultSet.next()) {
                    counter++;
                    if (isVerbose && counter % 1000 == 0) {
                        System.out.println("Processed " + counter);
                    }
                }
                long endTime = System.nanoTime();
                if (isVerbose) {
                    System.out.println("Finished " + query + " [" + counter + " rows]: took "
                            + TimeUnit.NANOSECONDS.toMillis(endTime - startTime) + " ms");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static long parallelScanShallow(Connection connection, String query, int threadCount, boolean isVerbose) {
        try {
            long startTime = System.nanoTime();
            int rowCount = Utils.getRowCount(connection);

            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);

            Utils.createRowNumRanges(threadCount, rowCount)
                    .map(range -> query + " WHERE " + range)
                    .forEach(q -> executorService.execute(() -> oneScanShallow(connection, q, isVerbose)));

            executorService.shutdown();
            boolean finished = executorService.awaitTermination(8, TimeUnit.HOURS);

            long endTime = System.nanoTime();
            long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            System.out.println(rowCount + " rows with " + threadCount + " threads took " + duration + " ms");

            return finished ? duration : Utils.RUN_FAILED;
        } catch (SQLException | InterruptedException e) {
            return Utils.RUN_FAILED;
        }
    }

    public static void main(String[] args) {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DriverManager.getConnection(url, user, password);

            // Utils.generateNewRowsBatch(connection, 10_000_000);

            String query = "select * from " + Utils.TABLE_NAME;

            String results = IntStream
                    .rangeClosed(1, 10) // thread count range
                    .boxed()
                    // .sorted(Collections.reverseOrder()) // to go from many threads to 1 thread
                    .map((threadCount) -> {
                        Double res = LongStream
                                .rangeClosed(1, 10) // iterations count range
                                .map((i) -> parallelScanShallow(connection, query, threadCount, false))
                                // RUN_FAILED == -1 is returned if a run failed with an exception
                                .filter((duration) -> duration > Utils.RUN_FAILED)
                                .boxed()
                                .collect(averagingLong(Long::valueOf));

                        return "ThreadsCount=" + threadCount + "; Average Duration=" + res + "ms";
                    })
                    .collect(joining("\n"));

            System.out.println("\n\n===================");
            System.out.println("ALL DONE:");
            System.out.println("===================\n\n");
            System.out.println(results);

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}