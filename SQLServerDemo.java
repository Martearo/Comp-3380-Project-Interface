import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Scanner;

public class SQLServerDemo {

    /**
     * Displays the ASCII art and welcome message, matching the style of the provided image.
     */
    private static void displayWelcomeScreen() {
        System.out.println("\n\n\n");
        System.out.println("> Welcome to NFL Database (2023-2024)");
        System.out.println("-------------------------------------\n");

        // Updated ASCII Art for NFL
        System.out.println("  _   _ _____ _    ");
        System.out.println(" | \\ | |  ___| |   ");
        System.out.println(" |  \\| | |__ | |   ");
        System.out.println(" | . ` |  __| | |  ");
        System.out.println(" | |\\  | |___|_|  ");
        System.out.println(" \\_| \\_/\\____(_)  ");
        
        System.out.println("\n> Type 'h' for help.");
        System.out.println("-------------------------------------\n");
    }

    private static void handleCommand(String command, Statement statement) throws SQLException {
        String lowerCommand = command.trim().toLowerCase();
        
        String[] parts = command.trim().split("\\s+");
        String baseCommand = parts[0].toLowerCase();

        if (baseCommand.equals("h") || baseCommand.equals("help")) {
            System.out.println("\nAvailable commands:");
            System.out.println("--------------------------------------------------------------------------");
            System.out.println("> h                  - Show this help menu.");
            System.out.println("> q                  - Exit the application.");
            System.out.println("> win                - Team won in the season.");
            System.out.println("> tds <player id>    - Get player touchdown score (tds).");
            System.out.println("> top <no of team>   - Get top scored team in the season.");
            System.out.println("> host <stadium>     - No. of games host by specific stadium.");
            System.out.println("> ypc <player name>  - Get the yard per carry (ypc) of specific player in season.");
            System.out.println("> score              - Get team score in that season.");
            System.out.println("> tdl                - Get touchdown leaders (tdl) for that season.");
            System.out.println("> tdp <week no.>     - Get total point differential in specific week.");
            System.out.println("--------------------------------------------------------------------------");
        } else if (baseCommand.equals("q") || baseCommand.equals("quit")) {
            System.out.println("\nExiting application. Goodbye!");
            System.exit(0);
        // } else if (baseCommand.equals("example")) {
        //     // Placeholder: Your original SQL query logic
        //     System.out.println("\n--- Running Example Query ---");
        //     String selectSql = "SELECT firstname, lastname, provinces.name from people join provinces on people.provinceID = provinces.provinceID;";
            
        //     try (ResultSet resultSet = statement.executeQuery(selectSql)) {
        //         while (resultSet.next()) {
        //             System.out.println(resultSet.getString(1) + 
        //                     " " + resultSet.getString(2) +
        //                     " lives in " + resultSet.getString(3));
        //         }
        //     }
        //     System.out.println("-----------------------------\n");
        } 
        else if (baseCommand.equals("win") || baseCommand.equals("tds") || baseCommand.equals("top") || 
                 baseCommand.equals("host") || baseCommand.equals("ypc") || baseCommand.equals("score") || 
                 baseCommand.equals("tdl") || baseCommand.equals("tdp")) {
            System.out.println("\nCommand '" + baseCommand + "' selected. (Logic not yet implemented. Type 'h' for help.)");
        }
        else {
            if (!lowerCommand.isEmpty()) {
                System.out.println("Unknown command: '" + command + "'. Type 'h' for help.");
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // --- 1. Load Configuration and Connect to Database ---
        Properties prop = new Properties();
        String fileName = "auth.cfg";
        String username;
        String password;

        // Use try-with-resources for the file stream, letting main's throws handle exceptions
        try (FileInputStream configFile = new FileInputStream(fileName)) {
            prop.load(configFile);
            username = prop.getProperty("username");
            password = prop.getProperty("password");
        }
        
        if (username == null || password == null) {
            System.err.println("\n[ERROR] Username or password not provided in auth.cfg.");
            throw new Exception("Configuration error: Username or password missing.");
        }

        String connectionUrl =
                "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
                + "database=cs3380;"
                + "user=" + username + ";"
                + "password="+ password +";"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement();
             Scanner scanner = new Scanner(System.in)) {

            // --- 2. Display Interface ---
            displayWelcomeScreen();

            // --- 3. Start Command Loop ---
            while (true) {
                // The main input prompt: "NFL >"
                System.out.print("NFL > ");
                
                if (scanner.hasNextLine()) {
                    String input = scanner.nextLine();
                    // handleCommand propagates exceptions via 'throws'
                    handleCommand(input, statement);
                } else {
                    break;
                }
            }
        }
    }
}