package database;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SimpleDatabase implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private static final Logger logger = Logger.getLogger(SimpleDatabase.class.getName());
    private String dbName;
    private Map<String, Table> tables;

    public SimpleDatabase(String dbName) {
        this.dbName = dbName;
        this.tables = new HashMap<>();
    }

    public void createTable(String tableName, List<String> columns) {
        Table table = new Table();
        table.createTable(columns);
        tables.put(tableName, table);
        System.out.println("Table \"" + tableName + "\" created.");
    }

    public void insertData(String tableName, List<String> values) {
        Table table = tables.get(tableName);
        if (table != null) {
            table.insertData(values);
            System.out.println("Data inserted into table \"" + tableName + "\".");
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
        }
    }

    public void insertDataWithColumns(String tableName, List<String> columns, List<String> values) {
        Table table = tables.get(tableName);
        if (table != null) {
            table.insertDataWithColumns(columns, values);
            System.out.println("Data inserted into table \"" + tableName + "\" with specified columns.");
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
        }
    }

    public List<Map<String, String>> selectData(String tableName, Map<String, String> conditions) {
        Table table = tables.get(tableName);
        if (table != null) {
            return table.selectData(conditions);
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
            return Collections.emptyList();
        }
    }

    public void updateData(String tableName, Map<String, String> setValues, Map<String, String> conditions) {
        Table table = tables.get(tableName);
        if (table != null) {
            table.updateData(setValues, conditions);
            System.out.println("Data updated in table \"" + tableName + "\".");
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
        }
    }

    public void deleteData(String tableName, Map<String, String> conditions) {
        Table table = tables.get(tableName);
        if (table != null) {
            table.deleteData(conditions);
            System.out.println("Data deleted from table \"" + tableName + "\".");
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
        }
    }

    public void dropTable(String tableName) {
        if (tables.containsKey(tableName)) {
            tables.remove(tableName);
            System.out.println("Table \"" + tableName + "\" dropped.");
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
        }
    }

    public List<Map<String, String>> selectColumns(String tableName, List<String> columns) {
        Table table = tables.get(tableName);
        if (table != null) {
            return table.selectColumns(columns);
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
            return Collections.emptyList();
        }
    }

    public List<Map<String, String>> selectDistinctData(String tableName) {
        Table table = tables.get(tableName);
        if (table != null) {
            return table.selectDistinctData();
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
            return Collections.emptyList();
        }
    }

    public List<Map<String, String>> selectDistinctColumns(String tableName, List<String> columns) {
        Table table = tables.get(tableName);
        if (table != null) {
            return table.selectDistinctColumns(columns);
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
            return Collections.emptyList();
        }
    }

    public int selectCount(String tableName, String column) {
        Table table = tables.get(tableName);
        if (table != null) {
            return table.selectCount(column);
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
            return 0;
        }
    }

    public List<Map<String, String>> selectCountGroupBy(String tableName, String column1, String column2) {
        Table table = tables.get(tableName);
        if (table != null) {
            return table.selectCountGroupBy(column1, column2);
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
            return Collections.emptyList();
        }
    }

    public List<Map<String, String>> selectCountGroupByOrderBy(String tableName, String column1, String column2) {
        Table table = tables.get(tableName);
        if (table != null) {
            return table.selectCountGroupByOrderBy(column1, column2);
        } else {
            System.out.println("Table \"" + tableName + "\" does not exist.");
            return Collections.emptyList();
        }
    }

    public void saveToFile(String fileName) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(fileName))) {
            for (Map.Entry<String, Table> entry : tables.entrySet()) {
                String tableName = entry.getKey();
                Table table = entry.getValue();

                writer.println(tableName);

                if (table.getColumns() != null && !table.getColumns().isEmpty()) {
                    writer.println(String.join(",", table.getColumns()));
                }

                for (Map<String, String> row : table.getData()) {
                    if (table.getColumns() != null && !table.getColumns().isEmpty()) {
                        // Ensure values are written in the correct order based on columns
                        String[] values = table.getColumns().stream()
                                .map(column -> row.getOrDefault(column, ""))
                                .toArray(String[]::new);
                        writer.println(String.join(",", values));
                    }
                }

                writer.println(); // Separate tables with an empty line
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error saving database to file: " + e.getMessage(), e);
        }
    }

    public void loadFromFile(String fileName) {
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            Table currentTable = null;
            List<String> columns = null;

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    // Empty line might indicate a separation between tables or end of file
                    currentTable = null;
                    continue;
                }
                if (currentTable == null) {
                    // The line should be the table name
                    String tableName = line.trim();
                    currentTable = new Table();
                    tables.put(tableName, currentTable);
                } else if (columns == null) {
                    // The line should contain column names
                    columns = Arrays.asList(line.split(","));
                    currentTable.createTable(columns);
                } else {
                    // The line should contain row data
                    List<String> values = Arrays.asList(line.split(","));
                    currentTable.insertDataWithColumns(columns, values);
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("File not found: " + fileName);
        } catch (IOException e) {
            System.out.println("Error reading file: " + e.getMessage());
        }
    }

}