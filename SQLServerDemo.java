import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;         
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLServerDemo {

    // IMPORTANT: Make sure this file exists and contains all necessary table definitions and data.
    private static final String NFL_SQL_FILE = "nfl.sql"; 
    
    private static final Scanner consoleScanner = new Scanner(System.in); 
    
    // --- CONSTANTS FOR PAGINATION ---
    private static final int PLAYER_PAGE_SIZE = 20; 
    private static final int TEAM_PAGE_SIZE = 10; 

    // --- Data Classes for Java Pagination ---
    private static class PlayerData {
        String playerId;
        String displayName;
        public PlayerData(String id, String name) {
            this.playerId = id;
            this.displayName = name;
        }
    }

    private static class TeamData {
        String teamAbbr;
        String teamName;
        String division;
        public TeamData(String abbr, String name, String div) {
            this.teamAbbr = abbr;
            this.teamName = name;
            this.division = div;
        }
    }

    private static void displayWelcomeScreen() {
        System.out.println("\n\n\n");
        System.out.println("Welcome to NFL Database (2023-2024)");
        System.out.println("-------------------------------------");
        // Corrected ASCII Logo
        System.out.println("  _  _ _____ _  ");
        System.out.println(" | \\ | | ___| |  ");
        System.out.println(" |  \\| | |__ | |  ");
        System.out.println(" | . ` | __| | |  ");
        System.out.println(" | |\\  | |  |_|_ ");
        System.out.println(" \\_| \\-| |___)  ");
        System.out.println("\n> Type 'h' for help or 'q' to quit.");
        System.out.println("-------------------------------------\n");
    }

    private static void loadSqlFile(Connection connection, String filename) throws IOException, SQLException {
        File sqlFile = new File(filename);
        if (!sqlFile.exists()) {
            throw new FileNotFoundException("File not found: " + filename);
        }

        Scanner scanner = new Scanner(sqlFile);

        StringBuilder currentStatement = new StringBuilder();
        int lineNumber = 0;
        int executedCount = 0;

        connection.setAutoCommit(false);

        try (Statement statement = connection.createStatement()) {

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                lineNumber++;

                // 👇👇👇 ADDED BREAK POINT CHECK 👇👇👇
                if (line.trim().equalsIgnoreCase("-- --- STOP EXECUTION HERE ---")) {
                    System.out.println("⚠️ **STOP MARKER REACHED.** Executing DROP commands now and halting file load.");
                    break; // Exit the while loop early
                }
                // 👆👆👆 ADDED BREAK POINT CHECK 👆👆👆

                String trimmed = line.trim();
                if (trimmed.startsWith("--") || trimmed.startsWith("/*") || trimmed.startsWith("*")) {
                    continue; // skip comments
                }

                currentStatement.append(line).append("\n");

                // End of SQL statement
                if (trimmed.endsWith(";")) {
                    String sql = currentStatement.toString().trim();

                    // Remove trailing semicolon
                    if (sql.endsWith(";")) {
                        sql = sql.substring(0, sql.length() - 1);
                    }

                    if (!sql.isEmpty()) {
                        try {
                            statement.addBatch(sql);
                            executedCount++;
                        } catch (SQLException ex) {
                            System.err.println("\n❌ SQL ERROR at line " + lineNumber);
                            System.err.println("Failed SQL statement:\n" + sql);
                            System.err.println("Error message: " + ex.getMessage());
                            throw ex; // rethrow to stop processing
                        }
                    }

                    currentStatement.setLength(0); // reset buffer
                }
            }

            // Execute all batched statements (the DROP commands)
            statement.executeBatch();
            connection.commit();

            System.out.println("✅ Successfully executed " + executedCount + " SQL statements from " + filename);

        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
            scanner.close();
        }
    }


    public static void main(String[] args) {
        Properties prop = new Properties();
        String authFileName = "auth.cfg";
        try {
            FileInputStream configFile = new FileInputStream(authFileName);
            prop.load(configFile);
            configFile.close();
        } catch (FileNotFoundException ex) {
            System.out.println("Could not find config file: " + authFileName);
            System.exit(1);
        } catch (IOException ex) {
            System.out.println("Error reading config file.");
            System.exit(1);
        }
        String username = (prop.getProperty("username"));
        String password = (prop.getProperty("password"));

        if (username == null || password == null){
            System.out.println("Username or password not provided in auth.cfg.");
            System.exit(1);
        }

        String connectionUrl =
                "jdbc:sqlserver://uranium.cs.umanitoba.ca:1433;"
                + "database=cs3380;"
                + "user=" + username + ";"
                + "password="+ password +";"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        // 2. Database Connection, SQL Load, and Input Loop
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {

            // --- Load the nfl.sql file ---
            try {
                loadSqlFile(connection, NFL_SQL_FILE);
            } catch (FileNotFoundException e) {
                System.err.println("❌ ERROR: The SQL file " + NFL_SQL_FILE + " was not found in the current directory.");
                System.err.println("The program will continue, but the database may not be initialized correctly.");
            } catch (IOException e) {
                System.err.println("❌ ERROR reading SQL file: " + e.getMessage());
            } catch (SQLException e) {
                 System.err.println("❌ ERROR executing SQL statements from " + NFL_SQL_FILE + ": " + e.getMessage());
            }
            // ------------------------------------

            // Display welcome screen
            displayWelcomeScreen();

            // Input loop
            String userInput = "";
            while (true) {
                System.out.print("NFL > ");
                userInput = consoleScanner.nextLine().trim(); 

                if (userInput.equalsIgnoreCase("q") || userInput.equalsIgnoreCase("quit")) {
                    System.out.println("\nExiting NFL Database. Goodbye!");
                    break;
                }
                else if (userInput.equalsIgnoreCase("h") || userInput.equalsIgnoreCase("help")) {
                    displayHelp();
                }
                else if (!userInput.isEmpty()) {
                    processCommand(connection, userInput);
                }
            }

        }
        catch (SQLException e) {
            System.out.println("\nDatabase connection error. Details:");
            e.printStackTrace();
        }
        catch (Exception e) {
            System.out.println("\nAn unexpected error occurred. Details:");
            e.printStackTrace();
        } 
    }

    // --- HELPER: For safe integer input ---
    private static int promptForInt(String prompt, int defaultValue) {
        System.out.print(prompt + (defaultValue != -1 ? " (Default: " + defaultValue + "): " : ": "));
        String input = consoleScanner.nextLine().trim();
        if (input.isEmpty() && defaultValue != -1) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException e) {
            System.err.println("❌ Invalid input. Please enter a whole number.");
            return promptForInt(prompt, defaultValue); // Recursively ask again
        }
    }
    
    // --- HELPER: For Season Type Menu Input ---
    private static int promptForSeasonType() {
        System.out.println("> [1] Regular Season");
        System.out.println("> [2] Post Season");
        System.out.print("Enter season type [1/2]: "); 
        
        String input = consoleScanner.nextLine().trim();
        try {
            int choice = Integer.parseInt(input);
            if (choice == 1 || choice == 2) {
                return choice;
            } else {
                System.err.println("❌ Invalid choice. Please enter 1 or 2.");
                return promptForSeasonType();
            }
        } catch (NumberFormatException e) {
            System.err.println("❌ Invalid input. Please enter 1 or 2.");
            return promptForSeasonType();
        }
    }
    
    // --- Helper to execute Statement (for simple, non-parameterized queries) ---
    private static void runSimpleQuery(Connection connection, String sql) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            printResultSet(rs);
        }
    }

    // --- HELPER METHOD FOR QUERY EXECUTION (PreparedStatement) ---
    private static void runQuery(PreparedStatement pStmt) throws SQLException {
        try (ResultSet rs = pStmt.executeQuery()) {
            printResultSet(rs);
        }
    }

    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Print Header
        System.out.print("   | ");
        for (int i = 1; i <= columnCount; i++) {
            // Using getColumnLabel for cleaner output
            System.out.printf("%-20s | ", rsmd.getColumnLabel(i));
        }
        System.out.println("\n---| " + "---------------------".repeat(columnCount));

        // Print Rows
        int rowCount = 0;
        while (rs.next()) {
            rowCount++;
            
            System.out.print(String.format("%2d| ", rowCount));
            
            for (int i = 1; i <= columnCount; i++) {
                String value = rs.getString(i);
                System.out.printf("%-20s | ", value == null ? "NULL" : value);
            }
            System.out.println();
        }

        if (rowCount == 0) {
            System.out.println("   | No results found.");
        }
        System.out.println("--- End of Query ---");
    }

    // --- PAGINATION LOGIC  ---
    // Note: This relies on fetching all data into memory first.
    private static void displayPagedPlayers(Connection connection) throws SQLException {
        
        // 1. Fetch ALL players from the database
        List<PlayerData> allPlayers = new ArrayList<>();
        String sql = "SELECT player_id, display_name FROM player ORDER BY display_name";
        System.out.println("-> Fetching all player data for pagination...");
        
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                allPlayers.add(new PlayerData(rs.getString("player_id"), rs.getString("display_name")));
            }
        }

        int totalPlayers = allPlayers.size();
        if (totalPlayers == 0) {
            System.out.println("No players found in the database.");
            return;
        }

        int totalPages = (int) Math.ceil((double) totalPlayers / PLAYER_PAGE_SIZE);
        int currentPage = 1;
        
        while (true) {
            // 2. Display prompt and calculate start/end index
            System.out.println("\nTotal players: " + totalPlayers + ". Total pages: " + totalPages + " (Size: " + PLAYER_PAGE_SIZE + " players/page)");
            String prompt = String.format("Enter page number (1 to %d, 'n' for next, 'p' for previous, 'q' to quit list) (Current: %d): ", totalPages, currentPage);
            System.out.print(prompt);
            String input = consoleScanner.nextLine().trim().toLowerCase();

            if (input.equals("q") || input.equals("quit")) {
                break;
            } else if (input.equals("n")) {
                currentPage = Math.min(currentPage + 1, totalPages);
            } else if (input.equals("p")) {
                currentPage = Math.max(currentPage - 1, 1);
            } else {
                try {
                    int pageNumber = Integer.parseInt(input);
                    if (pageNumber >= 1 && pageNumber <= totalPages) {
                        currentPage = pageNumber;
                    } else {
                        System.err.println("❌ Invalid page number. Must be between 1 and " + totalPages + ".");
                        continue;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("❌ Invalid input. Enter a number or 'n', 'p', or 'q'.");
                    continue;
                }
            }

            int startIndex = (currentPage - 1) * PLAYER_PAGE_SIZE;
            int endIndex = Math.min(startIndex + PLAYER_PAGE_SIZE, totalPlayers);
            
            // 3. Display the page
            System.out.println("\nDisplaying Page " + currentPage + " (Rows " + (startIndex + 1) + " to " + endIndex + ")");
            
            System.out.printf("   | %-20s | %-20s |%n", "PLAYER_ID", "DISPLAY_NAME");
            System.out.println("---|----------------------+----------------------|");
            
            int rowCount = 0;
            // Use Java's List subList for the current page
            List<PlayerData> currentPageData = allPlayers.subList(startIndex, endIndex);
            
            for (PlayerData player : currentPageData) {
                rowCount++;
                System.out.printf("%2d| %-20s | %-20s |%n", (startIndex + rowCount), player.playerId, player.displayName);
            }
            System.out.println("--- End of Page ---");
        }
    }
    
    private static void displayPagedTeams(Connection connection) throws SQLException {
        
        List<TeamData> allTeams = new ArrayList<>();
        String sql = "SELECT team_abbr, team_name, team_division FROM team ORDER BY team_name";
        System.out.println("-> Fetching all team data for pagination...");

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                allTeams.add(new TeamData(
                    rs.getString(1), // team_abbr
                    rs.getString(2), // team_name
                    rs.getString(3)  // team_division
                ));
            }
        }

        int totalTeams = allTeams.size();
        if (totalTeams == 0) {
            System.out.println("No teams found in the database.");
            return;
        }

        int totalPages = (int) Math.ceil((double) totalTeams / TEAM_PAGE_SIZE);
        int currentPage = 1;
        
        while (true) {
            // 2. Display prompt and calculate start/end index
            System.out.println("\nTotal teams: " + totalTeams + ". Total pages: " + totalPages + " (Size: " + TEAM_PAGE_SIZE + " teams/page)");
            String prompt = String.format("Enter page number (1 to %d, 'n' for next, 'p' for previous, 'q' to quit list) (Current: %d): ", totalPages, currentPage);
            System.out.print(prompt);
            String input = consoleScanner.nextLine().trim().toLowerCase();

            if (input.equals("q") || input.equals("quit")) {
                break;
            } else if (input.equals("n")) {
                currentPage = Math.min(currentPage + 1, totalPages);
            } else if (input.equals("p")) {
                currentPage = Math.max(currentPage - 1, 1);
            } else {
                try {
                    int pageNumber = Integer.parseInt(input);
                    if (pageNumber >= 1 && pageNumber <= totalPages) {
                        currentPage = pageNumber;
                    } else {
                        System.err.println("❌ Invalid page number. Must be between 1 and " + totalPages + ".");
                        continue;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("❌ Invalid input. Enter a number or 'n', 'p', or 'q'.");
                    continue;
                }
            }

            int startIndex = (currentPage - 1) * TEAM_PAGE_SIZE;
            int endIndex = Math.min(startIndex + TEAM_PAGE_SIZE, totalTeams);
            
            // 3. Display the page
            System.out.println("\nDisplaying Page " + currentPage + " (Rows " + (startIndex + 1) + " to " + endIndex + ")");
            
            System.out.printf("   | %-8s | %-20s | %-20s |%n", "ABBR", "TEAM_NAME", "DIVISION");
            System.out.println("---|----------+----------------------+----------------------|");
            
            int rowCount = 0;
            List<TeamData> currentPageData = allTeams.subList(startIndex, endIndex);
            
            for (TeamData team : currentPageData) {
                rowCount++;
                System.out.printf("%2d| %-8s | %-20s | %-20s |%n", (startIndex + rowCount), team.teamAbbr, team.teamName, team.division);
            }
            System.out.println("--- End of Page ---");
        }
    }


    // --- IMPLEMENTATION OF COMMAND PROCESSING ---
    private static void processCommand(Connection connection, String command) {
        // PreparedStatement is still used for parameterized queries
        PreparedStatement pStmt = null; 
        String[] parts = command.split("\\s+", 2);
        String action = parts[0].toLowerCase();
        String argument = parts.length > 1 ? parts[1].trim() : "";
        String sql;

        System.out.println("-> Executing command: " + command);

        try {
            // Command: all_players - Paged using Java List logic
            if (action.equals("all_players") || action.equals("all_plys")) {
                displayPagedPlayers(connection);
            }
            
            // --- all_teams - Paged using Java List logic ---
            else if (action.equals("all_teams") || action.equals("all_tms")) {
                displayPagedTeams(connection);
            }
            // --------------------------------------------------------------------------------
            
            // Command: win - Team won the championship (super bowl) in a specific season?
            else if (action.equals("win")) {
                int season = promptForInt("Enter Season Year for Super Bowl winner", 2023);

                sql = "SELECT t.team_name, p.season FROM post_team_stat p JOIN team t ON p.team = t.team_abbr WHERE p.finish = 'champ.win' AND p.season = ?";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                runQuery(pStmt);
            } 
            // Command: tds <player id> - Given a player id, get the number of touchdowns scored.
            else if (action.equals("tds")) {
                if (argument.isEmpty()) {
                    System.err.println("❌ Error: Missing player ID. Use 'all_players' to see IDs. Usage: tds <player id>");
                    return;
                }
                
                int season = promptForInt("Enter Season Year", 2023);

                sql = "SELECT p.display_name, (pps.passing_tds + pps.receiving_tds + pps.rushing_tds + pps.special_teams_tds) AS TouchDowns FROM player p JOIN post_player_stat pps ON p.player_id = pps.player_id WHERE p.player_id = ? AND pps.season = ?";
                
                pStmt = connection.prepareStatement(sql);
                pStmt.setString(1, argument);
                pStmt.setInt(2, season);
                
                try (ResultSet rs = pStmt.executeQuery()) {
                    if (rs.next()) {
                        printResultSet(rs);
                    } else {
                        String playerName = getPlayerName(connection, argument);
                        if (playerName == null) {
                            System.err.println("❌ Error: Player ID '" + argument + "' not found in the database.");
                        } else {
                            System.out.println("Player: " + playerName);
                            System.err.println("⚠️ Warning: No Post Season touchdown statistics found for this player in season " + season + ".");
                        }
                    }
                }
            } 
            // Command: top <no of team> - Get the top N teams in points scored.
            else if (action.equals("top")) {
                int limit = 3;
                try {
                    if (!argument.isEmpty()) {
                        limit = Integer.parseInt(argument);
                    }
                } catch (NumberFormatException e) {
                    System.err.println("⚠️ Warning: Invalid number. Defaulting to 3.");
                }

                int seasonType = promptForSeasonType();  // 1 = reg, 2 = post

                String table = "";
                String typeLabel = "";
                String statColumn = "";

                if (seasonType == 1) {
                    table = "reg_team_stat";
                    typeLabel = "Regular Season";
                    statColumn = "points_scored";   // ← Column exists only here
                } else {
                    table = "post_team_stat";
                    typeLabel = "Post Season";
                    statColumn = "passing_yards";   // ← Valid column for postseason
                }

                int season = promptForInt("Enter Season Year (" + typeLabel + ")", 2023);

                sql =
                    "SELECT TOP " + limit + " t.team_name, ts." + statColumn + " " +
                    "FROM " + table + " ts " +
                    "JOIN team t ON t.team_abbr = ts.team " +
                    "WHERE ts.season = ? " +
                    "ORDER BY ts." + statColumn + " DESC";

                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                runQuery(pStmt);
            }
            // Command: host <stadium name> - How many games were hosted by a certain stadium?
            else if (action.equals("host")) {
                if (argument.isEmpty()) {
                    System.err.println("❌ Error: Missing stadium name. Usage: host <stadium name>");
                    return;
                }
                int season = promptForInt("Enter Season Year", 2023);
                
                sql = "SELECT COUNT(g.game_id) AS gamesHosted FROM game g JOIN played_in pi ON g.game_id = pi.game_id JOIN stadium s ON pi.stadium_id = s.stadium_id WHERE g.game_type = 'post' AND s.stadium = ? AND g.season = ?";
                pStmt = connection.prepareStatement(sql);
                pStmt.setString(1, argument);
                pStmt.setInt(2, season);
                runQuery(pStmt);
            }
            // Command: ypc <player name> - What was the yards per carry (YPC) of a specific player?
            else if (action.equals("ypc")) {
                if (argument.isEmpty()) {
                    System.err.println("❌ Error: Missing player name. Usage: ypc <player name>");
                    return;
                }
                int season = promptForInt("Enter Season Year (Regular Season)", 2023);

                sql = "SELECT p.display_name, CAST((rps.rushing_yards + rps.receiving_yards + rps.passing_yards) AS DECIMAL(10,2)) / NULLIF(rps.carries, 0) AS YPC FROM player p JOIN reg_player_stat rps ON p.player_id = rps.player_id WHERE p.display_name = ? AND rps.season = ?";
                
                pStmt = connection.prepareStatement(sql);
                pStmt.setString(1, argument);
                pStmt.setInt(2, season);
                runQuery(pStmt);
            }
            // Command: score <team name> - How many points did a team score?
            else if (action.equals("score")) {
                if (argument.isEmpty()) {
                    System.err.println("❌ Error: Missing team name. Usage: score <team name>");
                    return;
                }
                int season = promptForInt("Enter Season Year (Post Season)", 2023);
                
                sql = "SELECT t.team_name, pts.points_scored FROM post_team_stat pts JOIN team t ON pts.team = t.team_abbr WHERE t.team_name = ? AND pts.season = ?";
                    
                pStmt = connection.prepareStatement(sql);
                pStmt.setString(1, argument);
                pStmt.setInt(2, season);
                runQuery(pStmt);
            }
            // Command: tdl - Return the touchdown leader at every jersey number for the regular season
            else if (action.equals("tdl")) {
                int season = promptForInt("Enter Season Year for Touchdown Leaders", 2023);
                
                sql = "WITH MaxTDsPerJersey AS ( "
                    + "    SELECT p.jersey_number, MAX(rps.passing_tds + rps.rushing_tds + rps.receiving_tds) AS max_tds "
                    + "    FROM reg_player_stat rps JOIN player p ON rps.player_id = p.player_id "
                    + "    WHERE p.jersey_number IS NOT NULL AND rps.season = ? "
                    + "    GROUP BY p.jersey_number "
                    + ") "
                    + "SELECT p.display_name, mtd.jersey_number, mtd.max_tds AS Touchdowns "
                    + "FROM MaxTDsPerJersey mtd "
                    + "JOIN player p ON mtd.jersey_number = p.jersey_number "
                    + "JOIN reg_player_stat rps ON p.player_id = rps.player_id "
                    + "WHERE mtd.max_tds = (rps.passing_tds + rps.rushing_tds + rps.receiving_tds) AND rps.season = ? "
                    + "ORDER BY mtd.jersey_number";

                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                pStmt.setInt(2, season);
                runQuery(pStmt);
            }
            // Command: tdp <week no.> - What is the total point differential of all games combined in a specific week?
            else if (action.equals("tdp")) {
                if (argument.isEmpty()) {
                    System.err.println("❌ Error: Missing week number. Usage: tdp <week no.>");
                    return;
                }
                try {
                    int week = Integer.parseInt(argument);
                    int season = promptForInt("Enter Season Year for point differential", 2023);
                    
                    // Building the query string for Statement (no ? needed)
                    sql = "SELECT SUM(home_score - away_score) AS Total_Point_Differential FROM game WHERE week = " + week + " AND game_type = 'reg' AND season = " + season;
                    
                    runSimpleQuery(connection, sql);
                } catch (NumberFormatException e) {
                    System.err.println("❌ Error: Week number must be an integer.");
                }
            }
            // Command: players_low_yds <max yards> <division>
            else if (action.equals("plyr_yds")) {
                Pattern p = Pattern.compile("^(\\d+)\\s+(.+)$");
                Matcher m = p.matcher(argument);

                if (!m.matches()) {
                    System.err.println("❌ Error: Invalid format. Usage: players_low_yds <max yards> <division name>");
                    return;
                }

                try {
                    int max_yards = Integer.parseInt(m.group(1));
                    String division = m.group(2);
                    int season = promptForInt("Enter Season Year for player stats", 2023);

                    sql = "SELECT p.display_name, (rps.receiving_yards + rps.passing_yards + rps.rushing_yards) AS total_yds "
                        + "FROM reg_player_stat rps "
                        + "JOIN player p ON rps.player_id = p.player_id "
                        + "JOIN roaster rstr ON p.player_id = rstr.player_id "
                        + "JOIN team t ON rstr.team = t.team_abbr "
                        + "JOIN reg_team_stat rts ON t.team_abbr = rts.team "
                        + "WHERE rts.division_rank IN (1, 2) "
                        + "AND t.team_division = ? "
                        + "AND rts.season = ? "
                        + "AND (rps.receiving_yards + rps.passing_yards + rps.rushing_yards) < ? "
                        + "ORDER BY total_yds DESC";
                    
                    pStmt = connection.prepareStatement(sql);
                    pStmt.setString(1, division);
                    pStmt.setInt(2, season);
                    pStmt.setInt(3, max_yards);
                    runQuery(pStmt);

                } catch (NumberFormatException e) {
                    System.err.println("❌ Error: Max yards must be a valid integer.");
                }
            }
            // Command: top_half_low_div
            else if (action.equals("top_half_low_div") || action.equals("hld")) {
                int season = promptForInt("Enter Season Year", 2023);
                
                sql = "SELECT t.team_name FROM reg_team_stat rts JOIN team t ON rts.team = t.team_abbr "
                    + "WHERE rts.season = ? "
                    + "AND rts.points_scored > (SELECT AVG(points_scored) FROM reg_team_stat WHERE season = ?) "
                    + "AND rts.division_rank IN (3, 4)";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                pStmt.setInt(2, season);
                runQuery(pStmt);
            }
            // Command: ref_away_win
            else if (action.equals("ref_away_win") || action.equals("ref_win")) {
                int season = promptForInt("Enter Season Year for Referee Stats", 2023);

                sql = "SELECT TOP 1 r.official_name, COUNT(o.game_id) AS gamesOfficiatedAwayWin "
                    + "FROM refree r JOIN official o ON r.official_id = o.official_id "
                    + "JOIN game g ON o.game_id = g.game_id "
                    + "WHERE g.away_score > g.home_score AND g.season = ? "
                    + "GROUP BY r.official_name "
                    + "ORDER BY gamesOfficiatedAwayWin DESC";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                runQuery(pStmt);
            }
            // Command: def_tds
            else if (action.equals("def_tds")) {
                int season = promptForInt("Enter Season Year for Player Stats", 2023);
                
                sql = "SELECT p.display_name, p.position, (rps.passing_tds + rps.receiving_tds + rps.rushing_tds + rps.special_teams_tds) AS defensive_tds "
                    + "FROM player p JOIN reg_player_stat rps ON p.player_id = rps.player_id "
                    + "WHERE p.position IN ('CB', 'S', 'LB', 'DE', 'DT') "
                    + "AND rps.season = ? "
                    + "AND (rps.passing_tds + rps.receiving_tds + rps.rushing_tds + rps.special_teams_tds) > 0";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                runQuery(pStmt);
            }
            // Command: win_pct <team name>
            else if (action.equals("win_pct")) {
                if (argument.isEmpty()) {
                    System.err.println("❌ Error: Missing team name. Use 'all_teams' to see names. Usage: win_pct <team name>");
                    return;
                }
                int season = promptForInt("Enter Season Year for Win Percentage", 2023);

                sql = "SELECT t.team_abbr, CAST(rts.wins AS DECIMAL(10,2)) / NULLIF((rts.wins + rts.losses), 0) AS win_pct "
                    + "FROM team t JOIN reg_team_stat rts ON t.team_abbr = rts.team "
                    + "WHERE t.team_name = ? AND rts.season = ?";
                pStmt = connection.prepareStatement(sql);
                pStmt.setString(1, argument);
                pStmt.setInt(2, season);
                runQuery(pStmt);
            }
            // Command: ref_penalties <team abbr>
            else if (action.equals("ref_penalties") || action.equals("ref_pen")) {
                if (argument.isEmpty()) {
                    System.err.println("❌ Error: Missing team abbreviation. Use 'all_teams' to see abbreviations. Usage: ref_penalties <team abbr>");
                    return;
                }
                String teamAbbr = argument.toUpperCase();
                int season = promptForInt("Enter Season Year for Penalty Stats", 2023);

                sql = "WITH refOfficiated AS ( "
                    + "    SELECT r.official_id, r.official_name, COUNT(g.game_id) AS gamesOfficiated "
                    + "    FROM refree r JOIN official o ON r.official_id = o.official_id "
                    + "    JOIN game g ON o.game_id = g.game_id "
                    + "    WHERE (g.home_team = ? OR g.away_team = ?) "
                    + "    AND g.season = ? " 
                    + "    GROUP BY r.official_id, r.official_name "
                    + "), "
                    + "targetRef AS ( "
                    + "    SELECT official_name, gamesOfficiated AS max_games "
                    + "    FROM refOfficiated "
                    + "    WHERE gamesOfficiated = (SELECT MAX(gamesOfficiated) FROM refOfficiated) "
                    + ") "
                    + "SELECT t.team_abbr, rts.penalties, tr.official_name, tr.max_games "
                    + "FROM reg_team_stat rts JOIN team t ON rts.team = t.team_abbr "
                    + "CROSS JOIN targetRef tr "
                    + "WHERE t.team_abbr = ? AND rts.season = ?"; 

                pStmt = connection.prepareStatement(sql);
                pStmt.setString(1, teamAbbr);
                pStmt.setString(2, teamAbbr);
                pStmt.setInt(3, season);
                pStmt.setString(4, teamAbbr);
                pStmt.setInt(5, season);
                runQuery(pStmt);
            }
            // Command: low_targets
            else if (action.equals("low_targets") || action.equals("low_trgts")) {
                int season = promptForInt("Enter Season Year for Player Stats", 2023);

                sql = "SELECT p.display_name, rps.targets, rps.receptions "
                    + "FROM player p JOIN reg_player_stat rps ON p.player_id = rps.player_id "
                    + "WHERE rps.targets > rps.receptions AND rps.season = ?";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                runQuery(pStmt);
            }
            // Command: top5_post_tds
            else if (action.equals("top5_post_tds") || action.equals("top5_tds")) {
                int season = promptForInt("Enter Season Year for Postseason TDs", 2023);
                
                sql = "SELECT TOP 5 p.display_name, (pps.passing_tds + pps.receiving_tds + pps.rushing_tds + pps.special_teams_tds) AS Touchdowns "
                    + "FROM player p JOIN post_player_stat pps ON p.player_id = pps.player_id "
                    + "WHERE pps.season = " + season
                    + " ORDER BY Touchdowns DESC";
                
                runSimpleQuery(connection, sql);
            }
            // Command: defensive_trifecta
            else if (action.equals("defensive_trifecta") || action.equals("DFT")) {
                int season = promptForInt("Enter Season Year for Stats", 2023);
                
                sql = "SELECT p.display_name, p.position FROM player p JOIN reg_player_stat rps ON p.player_id = rps.player_id WHERE rps.sacks >= 1 AND rps.sack_fumbles >= 1 AND rps.interceptions >= 1 AND rps.season = ?";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                runQuery(pStmt);
            }
            // Command: shutouts
            else if (action.equals("shutouts")) {
                int season = promptForInt("Enter Season Year for Shutouts", 2023);

                sql = "SELECT t.team_abbr, g.week, g.season "
                    + "FROM team t "
                    + "JOIN game g ON (t.team_abbr = g.home_team AND g.home_score = 0) OR (t.team_abbr = g.away_team AND g.away_score = 0) "
                    + "WHERE g.season = ? "
                    + "ORDER BY g.week, t.team_abbr";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                runQuery(pStmt);
            }
            // Command: week_scores
            else if (action.equals("week_scores")) {
                int season = promptForInt("Enter Season Year for Week Scores", 2023);
                
                sql = "WITH GameScores AS ( "
                    + "    SELECT week, home_score AS score FROM game WHERE game_type = 'reg' AND season = ? "
                    + "    UNION ALL "
                    + "    SELECT week, away_score AS score FROM game WHERE game_type = 'reg' AND season = ? "
                    + ") "
                    + "SELECT gs.week, MAX(gs.score) AS MaxScore, MIN(gs.score) AS MinScore "
                    + "FROM GameScores gs GROUP BY gs.week ORDER BY gs.week";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                pStmt.setInt(2, season);
                runQuery(pStmt);
            }
            // Command: team_top_scorer
            else if (action.equals("team_top_scorer") || action.equals("top_scorer")) {
                int season = promptForInt("Enter Season Year for Top Scorers", 2023);
                
                sql = "WITH PlayerPoints AS ( "
                    + "    SELECT "
                    + "        rps.player_id, r.team, p.display_name, "
                    + "        ((rps.receiving_tds + rps.passing_tds + rps.rushing_tds + rps.special_teams_tds) * 6) + "
                    + "        ((rps.rushing_2pt_conversions + rps.receiving_2pt_conversions + rps.passing_2pt_conversions) * 2) AS player_points "
                    + "    FROM reg_player_stat rps JOIN player p ON rps.player_id = p.player_id "
                    + "    WHERE rps.season = ? "
                    + "), "
                    + "MaxOutput AS ( "
                    + "    SELECT r.team, MAX(pp.player_points) AS max_points "
                    + "    FROM roaster r JOIN PlayerPoints pp " 
                    + "    ON r.player_id = pp.player_id GROUP by r.team "
                    + ") "
                    + "SELECT r.team, pp.display_name, mo.max_points "
                    + "FROM roaster r JOIN PlayerPoints pp "
                    + "ON r.player_id = pp.player_id JOIN MaxOutput mo "
                    + "ON r.team = mo.team AND pp.player_points = mo.max_points "
                    + "ORDER BY r.team";
                pStmt = connection.prepareStatement(sql);
                pStmt.setInt(1, season);
                runQuery(pStmt);
            }
            // Command: h or help - Display help
            else if (action.equals("h") || action.equals("help")) {
                displayHelp();
            }
            // Command: q or quit - Exit the program
            else if (action.equals("q") || action.equals("quit")) {
                 System.out.println("\nExiting NFL Database. Goodbye!");
                 System.exit(0);
            }
            // Unknown command
            else {
                System.out.println("-> Command not recognized. Type 'h' for help.");
            }
        } catch (SQLException e) {
            System.err.println("❌ SQL Execution Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("❌ An unexpected error occurred while processing command: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the prepared statement if it was opened
            if (pStmt != null) {
                try {
                    pStmt.close();
                } catch (SQLException e) {
                    System.err.println("Error closing PreparedStatement: " + e.getMessage());
                }
            }
        }
    }

    // --- HELPER: Get player name for better error reporting ---
    private static String getPlayerName(Connection connection, String playerId) throws SQLException {
        String sql = "SELECT display_name FROM player WHERE player_id = '" + playerId + "'";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null;
    }

    private static void displayHelp() {
        System.out.println("\n\n🏈 NFL Database Command Reference (Type 'q' to quit)");
        System.out.println("----------------------------------------------------------------------------------------------------------------------------------");
        
        final String format = "| %-32s | %-12s | %-80s |%n";
        
        System.out.printf(format, "COMMAND (and Arguments)", "SHORT FORM", "DESCRIPTION");
        System.out.println("---------------------------------+--------------+----------------------------------------------------------------------------------|");

        // Print core commands
        System.out.printf(format, "all_players", "[ALL_PLYS]", "See all players and their IDs (Uses Java-based pagination).");
        // FIX: Updated description
        System.out.printf(format, "all_teams", "[ALL_TMS]", "See all teams, abbreviations, and divisions (Uses Java-based pagination)."); 
        System.out.printf(format, "win", "[WIN]", "Find the Super Bowl winning team in a given season (prompts for year).");

        // Print commands requiring player/team/location lookup
        System.out.printf(format, "tds <player id>", "[TDS]", "Get player postseason touchdown score (prompts for year).");
        System.out.printf(format, "ypc <player name>", "[YPC]", "Get the regular season yard per carry of a specific player (prompts for year).");
        System.out.printf(format, "score <team name>", "[SCORE]", "Get team total score in the postseason (prompts for year).");
        System.out.printf(format, "win_pct <team name>", "[WIN_PCT]", "What was the regular season win percentage of a given team (prompts for year).");
        System.out.printf(format, "host <stadium name>", "[HOST]", "Number of games hosted by a specific stadium in the postseason (prompts for year).");
        System.out.printf(format, "ref_penalties <team abbr>", "[REF_PEN]", "Get team penalties and their most frequent referee (prompts for year).");
        
        // Print statistical ranking/filtering commands
        System.out.printf(format, "top <no of team>", "[TOP]", "Get top N teams in regular season points scored.");
        System.out.printf(format, "tdl", "[TDL]", "Get touchdown leaders at every jersey number (prompts for year).");
        System.out.printf(format, "top5_post_tds", "[TOP5_TDS]", "Get the top 5 players in total touchdowns scored in the post-season.");
        System.out.printf(format, "def_tds", "[DEF_TDS]", "Which defensive players had a touchdown in the regular season (prompts for year).");
        System.out.printf(format, "defensive_trifecta", "[DFT]", "Players who recorded a sack, fumble, and interception in the regular season (prompts for year).");
        System.out.printf(format, "low_targets", "[LOW_TRGTS]", "Players with more targets than receptions in the regular season (prompts for year).");
        System.out.printf(format, "team_top_scorer", "[TOP_SCORER]", "Get the #1 scoring player on each team in the regular season (prompts for year).");

        // Print game/week analysis commands
        System.out.printf(format, "tdp <week no.>", "[TDP]", "Get total regular season point differential of all games combined in a specific week (prompts for year).");
        System.out.printf(format, "week_scores", "[WK_SCORE]", "Each regular season week's max and min points scored in a game (prompts for year).");
        System.out.printf(format, "shutouts", "[SHUTOUTS]", "Teams shut-out (scored zero) in a game, along with the week(s) it happened (prompts for year).");
        System.out.printf(format, "ref_away_win", "[REF_WIN]", "Referee who officiated the most games where the away team won (prompts for year).");

        // Print complex filtering commands
        System.out.printf(format, "plyr_yds <max yds> <division>", "[PLYR_YDS]", "Players on a top 2 division team with total yards < max yards (prompts for year).");
        System.out.printf(format, "top_half_low_div", "[HLD]", "Teams in the top half of the league in points but in the bottom half of their division (prompts for year).");

        // Print help/quit
        System.out.printf(format, "h | help", "[H]", "Display this help screen.");
        System.out.printf(format, "q | quit", "[Q]", "Exit the program.");
        System.out.println("----------------------------------------------------------------------------------------------------------------------------------");
    }
}
