package database;

import java.io.Serializable;
import java.util.*;

public class Table implements Serializable {
    private List<Map<String, String>> data;
    private List<String> columns;

    public void createTable(List<String> columns) {
        this.columns = columns;
        this.data = new ArrayList<>();
        System.out.println("Table created with columns: " + columns);
    }

    public void insertData(List<String> values) {
        Map<String, String> row = new LinkedHashMap<>();
        for (int i = 0; i < values.size(); i++) {
            row.put("Column" + (i + 1), values.get(i));
        }
        data.add(row);
        System.out.println("Data inserted: " + row);
    }

    public void insertDataWithColumns(List<String> columns, List<String> values) {
        if (columns.size() == values.size()) {
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < columns.size(); i++) {
                row.put(columns.get(i), values.get(i));
            }
            data.add(row);
            System.out.println("Data inserted with specified columns: " + row);
        } else {
            System.out.println("Number of columns and values does not match.");
        }
    }

    public List<Map<String, String>> selectData(Map<String, String> conditions) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> row : data) {
            if (matchConditions(row, conditions)) {
                result.add(row);
            }
        }
        return result;
    }

    public void updateData(Map<String, String> setValues, Map<String, String> conditions) {
        for (Map<String, String> row : data) {
            if (matchConditions(row, conditions)) {
                row.putAll(setValues);
            }
        }
    }

    public void deleteData(Map<String, String> conditions) {
        data.removeIf(row -> matchConditions(row, conditions));
    }

    private boolean matchConditions(Map<String, String> row, Map<String, String> conditions) {
        if (conditions == null || conditions.isEmpty()) {
            return true;
        }

        for (Map.Entry<String, String> entry : conditions.entrySet()) {
            String column = entry.getKey();
            String expectedValue = entry.getValue();

            if (row.containsKey(column) && !row.get(column).equals(expectedValue)) {
                return false;
            }
        }

        return true;
    }

    public List<Map<String, String>> selectColumns(List<String> columns) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> row : data) {
            Map<String, String> selectedRow = new LinkedHashMap<>();
            for (String column : columns) {
                selectedRow.put(column, row.get(column));
            }
            result.add(selectedRow);
        }
        return result;
    }

    public List<Map<String, String>> selectDistinctData() {
        Set<Map<String, String>> distinctRows = new LinkedHashSet<>(data);
        return new ArrayList<>(distinctRows);
    }

    public List<Map<String, String>> selectDistinctColumns(List<String> columns) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> row : data) {
            Map<String, String> selectedRow = new LinkedHashMap<>();
            for (String column : columns) {
                selectedRow.put(column, row.get(column));
            }
            if (!result.contains(selectedRow)) {
                result.add(selectedRow);
            }
        }
        return result;
    }

    public int selectCount(String column) {
        Set<String> uniqueValues = new HashSet<>();
        for (Map<String, String> row : data) {
            uniqueValues.add(row.get(column));
        }
        return uniqueValues.size();
    }

    public List<Map<String, String>> selectCountGroupBy(String column1, String column2) {
        Map<String, Integer> countMap = new HashMap<>();
        for (Map<String, String> row : data) {
            String key = row.get(column2);
            countMap.put(key, countMap.getOrDefault(key, 0) + 1);
        }

        List<Map<String, String>> result = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
            Map<String, String> row = new LinkedHashMap<>();
            row.put(column1, String.valueOf(entry.getValue()));
            row.put(column2, entry.getKey());
            result.add(row);
        }

        return result;
    }

    public List<Map<String, String>> selectCountGroupByOrderBy(String column1, String column2) {
        List<Map<String, String>> groupedData = selectCountGroupBy(column1, column2);

        // Sort the result by the specified column
        groupedData.sort((row1, row2) -> {
            String value1 = row1.get(column2);
            String value2 = row2.get(column2);
            return value1.compareTo(value2);
        });

        return groupedData;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Map<String, String>> getData() {
        return data;
    }
}
