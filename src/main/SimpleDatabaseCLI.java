package main;

import java.util.*;
import java.util.Map;
import java.util.stream.Collectors;
// Import statements for classes in the same package
import database.*;


public class SimpleDatabaseCLI {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        SimpleDatabase db = new SimpleDatabase("my_database");

        while (true) {
            System.out.print("Enter command (or type 'exit' to quit): ");
            String command = scanner.nextLine().trim();

            if (command.equalsIgnoreCase("exit")) {
                break;
            }

            processCommand(db, command);
        }

        scanner.close();
    }

    private static void processCommand(SimpleDatabase db, String command) {
        // Split the command into tokens
        String[] tokens = command.split("\\s+");
        String keyword = tokens[0].toLowerCase();

        switch (keyword) {
            case "create":
                processCreateCommand(db, tokens);
                break;

            case "insert":
                processInsertCommand(db, tokens);
                break;

            case "select":
                processSelectCommand(db, tokens);
                break;

            case "update":
                processUpdateCommand(db, tokens);
                break;

            case "delete":
                processDeleteCommand(db, tokens);
                break;

            case "drop":
                processDropCommand(db, tokens);
                break;

            case "save":
                db.saveToFile("my_database.txt");
                System.out.println("Data saved to file.");
                break;

            case "load":
                db.loadFromFile("my_database.txt");
                System.out.println("Data loaded from file.");
                break;

            default:
                System.out.println("Unknown command.");
        }
    }

    private static void processCreateCommand(SimpleDatabase db, String[] tokens) {
        String commandString = String.join(" ", tokens).toLowerCase();
        if (commandString.matches("create table \\w+ \\(.*\\);")) {
            String tableName = tokens[2];

            // Extract columns using a regular expression
            String columnsString = commandString.replaceAll("create table \\w+ \\((.*)\\);", "$1");
            List<String> columns = Arrays.asList(columnsString.split("\\s*,\\s*"));

            db.createTable(tableName, columns);
        } else {
            System.out.println("Invalid syntax. Usage: CREATE TABLE <table_name> (<column1>, <column2>, ...);");
        }
    }

    private static void processInsertCommand(SimpleDatabase db, String[] tokens) {
        if (tokens.length >= 4 && tokens[1].equalsIgnoreCase("into")) {
            String tableName = tokens[2];

            // Find the index of "values"
            int valuesStartIndex = -1;
            for (int i = 3; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("values")) {
                    valuesStartIndex = i;
                    break;
                }
            }

            if (valuesStartIndex != -1 && tokens[valuesStartIndex + 1].startsWith("(")) {
                // Process INSERT INTO table_name (column1, column2, ...) VALUES (value1, value2, ...);
                List<String> columns = extractValues(tokens, 3, valuesStartIndex - 1);
                List<String> values = extractValues(tokens, valuesStartIndex + 1, tokens.length - 1);
                db.insertDataWithColumns(tableName, columns, values);
            } else if (tokens[3].equalsIgnoreCase("values")) {
                // Process INSERT INTO table_name VALUES (value1, value2, ...);
                List<String> values = extractValues(tokens, 4, tokens.length - 1);
                db.insertData(tableName, values);
            } else {
                System.out.println("Invalid syntax. Usage: INSERT INTO <table_name> [ (column1, column2, ...) ] VALUES (value1, value2, ...)");
            }
        } else {
            System.out.println("Invalid syntax. Usage: INSERT INTO <table_name> VALUES (value1, value2, ...)");
        }
    }

    private static List<String> extractValues(String[] tokens, int start, int end) {
        List<String> values = new ArrayList<>();
        StringBuilder valueBuilder = new StringBuilder();

        for (int i = start; i <= end; i++) {
            valueBuilder.append(tokens[i]);
            if (i == end || tokens[i].endsWith(")") || tokens[i].endsWith(",")) {
                String value = valueBuilder.toString().replaceAll("[()]", ""); // Remove parentheses
                values.addAll(Arrays.asList(value.split(",")));
                valueBuilder = new StringBuilder();
            }
        }

        return values.stream().map(String::trim).collect(Collectors.toList());
    }

    private static void processSelectCommand(SimpleDatabase db, String[] tokens) {
        // Process SELECT command based on different patterns

        if (tokens.length >= 4 && tokens[1].equalsIgnoreCase("*") && tokens[2].equalsIgnoreCase("from")) {
            // SELECT * FROM table_name
            String tableName = tokens[3];
            System.out.println(db.selectData(tableName, null));
        } else if (tokens.length >= 4 && tokens[1].equalsIgnoreCase("distinct") && tokens[2].equalsIgnoreCase("*") && tokens[3].equalsIgnoreCase("from")) {
            // SELECT DISTINCT * FROM table_name
            String tableName = tokens[4];
            System.out.println(db.selectDistinctData(tableName));
        } else if (tokens.length >= 4 && tokens[1].equalsIgnoreCase("distinct") && tokens[tokens.length - 2].equalsIgnoreCase("from")) {
            // SELECT DISTINCT column1, column2, ... FROM table_name
            String tableName = tokens[tokens.length - 1];
            List<String> columns = Arrays.asList(tokens).subList(2, tokens.length - 2);
            System.out.println(db.selectDistinctColumns(tableName, columns));
        } else if (tokens.length >= 4 && tokens[tokens.length - 2].equalsIgnoreCase("from")) {
            // SELECT column1, column2, ... FROM table_name
            String tableName = tokens[tokens.length - 1];
            List<String> columns = Arrays.asList(tokens).subList(1, tokens.length - 2);
            System.out.println(db.selectColumns(tableName, columns));
        } else if (tokens.length == 4 && tokens[0].equalsIgnoreCase("select") && tokens[1].toLowerCase().startsWith("count(") && tokens[1].endsWith(")") && tokens[2].equalsIgnoreCase("from")) {
            // SELECT COUNT(column) FROM table_name;
            String tableName = tokens[3];
            String column = tokens[1].substring(6, tokens[1].length() - 1); // Extract column name
            System.out.println(db.selectCount(tableName, column));
        } else if (tokens.length == 7 && tokens[0].equalsIgnoreCase("select") && tokens[1].toLowerCase().startsWith("count(") && tokens[1].endsWith("),") && tokens[3].equalsIgnoreCase("from") && tokens[5].equalsIgnoreCase("group") && tokens[6].equalsIgnoreCase("by")) {
            // SELECT COUNT(column1), column2 FROM table_name GROUP BY column2;
            String tableName = tokens[4];
            String column1 = tokens[1].substring(6, tokens[1].length() - 2); // Extract column1 name
            String column2 = tokens[2]; // column2 name
            System.out.println(db.selectCountGroupBy(tableName, column1, column2));
        } else if (tokens.length >= 11 && tokens[1].equalsIgnoreCase("count") && tokens[2].equalsIgnoreCase("(") && tokens[3].contains(")") && tokens[5].equalsIgnoreCase("from") && tokens[7].equalsIgnoreCase("group") && tokens[8].equalsIgnoreCase("by") && tokens[10].equalsIgnoreCase("order")) {
            // SELECT COUNT(column1), column2 FROM table_name GROUP BY column2 ORDER BY column2
            String tableName = tokens[6];
            String column1 = tokens[3].substring(0, tokens[3].indexOf(")")); // Extracting column1 name
            String column2 = tokens[4];
            System.out.println(db.selectCountGroupByOrderBy(tableName, column1, column2));
        } else if (tokens.length >= 5 && tokens[1].equalsIgnoreCase("from") && tokens[3].equalsIgnoreCase("where")) {
            // SELECT * FROM table_name WHERE column1=value1 AND column2 LIKE value2 OR column3=value3;
            String tableName = tokens[2];
            Map<String, String> conditions = parseConditions(tokens, 4);
            System.out.println(db.selectData(tableName, conditions));
        } else {
            System.out.println("Invalid syntax. Usage: SELECT ... FROM ... [WHERE ...];");
        }
    }



    private static void processUpdateCommand(SimpleDatabase db, String[] tokens) {
        if (tokens.length >= 4 && tokens[0].equalsIgnoreCase("update")) {
            String tableName = tokens[1];

            int setIndex = -1;
            int whereIndex = -1;

            // Find the index of "SET" and "WHERE"
            for (int i = 2; i < tokens.length; i++) {
                if (tokens[i].equalsIgnoreCase("set") && setIndex == -1) {
                    setIndex = i;
                } else if (tokens[i].equalsIgnoreCase("where")) {
                    whereIndex = i;
                    break;
                }
            }

            if (setIndex != -1) {
                Map<String, String> setValues = parseSetValues(tokens, setIndex + 1, whereIndex - 1);
                Map<String, String> conditions = parseConditions(tokens, whereIndex + 1, tokens.length - 1);
                db.updateData(tableName, setValues, conditions);
            } else {
                System.out.println("Invalid syntax. Usage: UPDATE <table_name> SET <column1=value1, column2=value2, ...> [WHERE <condition>];");
            }
        } else {
            System.out.println("Invalid syntax. Usage: UPDATE <table_name> SET <column1=value1, column2=value2, ...> [WHERE <condition>];");
        }
    }

    private static void processDeleteCommand(SimpleDatabase db, String[] tokens) {
        if (tokens.length >= 4 && tokens[1].equalsIgnoreCase("from") && tokens[tokens.length - 2].equalsIgnoreCase("where")) {
            // DELETE FROM table_name WHERE condition;
            String tableName = tokens[2];
            Map<String, String> conditions = parseConditions(tokens, tokens.length - 1);
            db.deleteData(tableName, conditions);
        } else if (tokens.length == 3 && tokens[1].equalsIgnoreCase("from")) {
            // DELETE FROM table_name;
            String tableName = tokens[2];
            db.deleteData(tableName, null);
        } else {
            System.out.println("Invalid syntax. Usage: DELETE FROM <table_name> [WHERE <condition>];");
        }
    }

    private static void processDropCommand(SimpleDatabase db, String[] tokens) {
        if (tokens.length == 3 && tokens[1].equalsIgnoreCase("table")) {
            // DROP TABLE table_name;
            String tableName = tokens[2];
            db.dropTable(tableName);
        } else {
            System.out.println("Invalid syntax. Usage: DROP TABLE <table_name>;");
        }
    }

    private static Map<String, String> parseSetValues(String[] tokens, int start, int end) {
        Map<String, String> setValues = new HashMap<>();
        StringBuilder currentToken = new StringBuilder();

        for (int i = start; i <= end; i++) {
            currentToken.append(tokens[i]);
            if (i == end || tokens[i].endsWith(",")) {
                String[] keyValue = currentToken.toString().split("=");
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim().replaceAll(",$", ""); // Remove trailing comma
                    setValues.put(key, value);
                }
                currentToken = new StringBuilder(); // Reset for next key-value pair
            }
        }
        return setValues;
    }

    private static Map<String, String> parseConditions(String[] tokens, int startIndex, int... endIndex) {
        int end;
        if (endIndex.length > 0) {
            end = endIndex[0];
        } else {
            end = tokens.length;
        }

        Map<String, String> conditions = new HashMap<>();
        boolean isAND = true; // Start with AND as the default logical operator

        for (int i = startIndex; i < end; i++) {
            if (tokens[i].equalsIgnoreCase("AND")) {
                isAND = true;
            } else if (tokens[i].equalsIgnoreCase("OR")) {
                isAND = false;
            } else if (!tokens[i].equalsIgnoreCase("WHERE")) {
                String[] keyValue = tokens[i].split("=");
                if (keyValue.length == 2) {
                    String key = keyValue[0].trim();
                    String value = keyValue[1].trim();

                    if (isAND) {
                        conditions.put(key, value);
                    } else {
                        // Handle OR condition (combine with the previous condition using OR)
                        if (conditions.containsKey(key)) {
                            String existingValue = conditions.get(key);
                            conditions.put(key, existingValue + " OR " + value);
                        } else {
                            conditions.put(key, value);
                        }
                    }
                } else if (keyValue.length == 3 && keyValue[1].equalsIgnoreCase("LIKE")) {
                    String key = keyValue[0].trim();
                    String value = keyValue[2].trim();

                    if (isAND) {
                        conditions.put(key, "LIKE " + value);
                    } else {
                        // Handle OR condition for LIKE (combine with the previous condition using OR)
                        if (conditions.containsKey(key)) {
                            String existingValue = conditions.get(key);
                            conditions.put(key, existingValue + " OR LIKE " + value);
                        } else {
                            conditions.put(key, "LIKE " + value);
                        }
                    }
                } else {
                    break;
                }
            }
        }
        return conditions;
    }
}
