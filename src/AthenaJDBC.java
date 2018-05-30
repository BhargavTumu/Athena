import java.sql.*;
import java.util.Properties;


public class AthenaJDBC {

    private static final String athenaUrl = "jdbc:awsathena://AwsRegion=us-east-1";
    private static final String awsAccessKey = "XXXXXXXXXXXXXXXXXXXXX";
    private static final String awsSecretKey = "XXXXXXXXXXXXXXXXXXXXXXXXXX";
    private static final String S3OutputLocation	= "s3://aws-athena-query-results-btpersonal-test/";

    private static final String DatabaseName = "testdb";

    private static Connection conn = null;
    private static Statement statement = null;

    public static void main(String args[]) {

        Properties athenaProperties = new Properties();
        athenaProperties.put("UID", awsAccessKey);
        athenaProperties.put("PWD", awsSecretKey);
        athenaProperties.put("S3OutputLocation", S3OutputLocation);
        athenaProperties.put("LogPath", "/Users/myUser/athenaLog");
        athenaProperties.put("LogLevel","6");

        try {

            Class.forName("com.simba.athena.jdbc.Driver");
            System.out.println("Connecting to Athena...");
            conn = DriverManager.getConnection(athenaUrl, athenaProperties);
            System.out.println("Listing tables...");
            String sql = "show tables in "+ DatabaseName;

            statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                //Retrieve table column.
                String name = rs.getString("tab_name");
                //Display values.
                System.out.println("Name: " + name);
            }
            rs.close();
            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (Exception ex) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        System.out.println("Finished connectivity test.");

    }
}
