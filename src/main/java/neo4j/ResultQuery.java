package neo4j;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 09/07/2018.
 *
 */

public class ResultQuery {
	String[] columnsName;
	int columnsCount;
	List<List<ResultEntity>> dataList;
	int maxRowCount;

	public ResultQuery(int columnsCount) {
		this.columnsCount = columnsCount;
		this.maxRowCount = 0;

		dataList = new ArrayList<>();
		columnsName = new String[columnsCount];

	}

	public void setColumnsName(List<String> columnsName) {
		this.columnsName = (String[]) columnsName.toArray();
	}

	public int getColumnsCount() {
		return columnsCount;
	}

	public void setColumnsCount(int columnsCount) {
		this.columnsCount = columnsCount;
	}


	public String[] getColumnsName() {
		return columnsName;
	}


	public List<List<ResultEntity>> getDataList() {
		return dataList;
	}

	public void setDataList(List<List<ResultEntity>> dataList) {
		this.dataList = dataList;
	}

	public List<ResultEntity> getColumn(int columnIndex) {
		if (dataList != null) return dataList.get(columnIndex);
		return null;
	}

	public void addColumn(int columnIndex, String columnName) {
		if (columnsName != null && dataList != null) {
			System.out.println("Add new column");
			dataList.add(columnIndex, new ArrayList<ResultEntity>());
			columnsName[columnIndex] = columnName;
		}
	}

	public void addEntity(int columnIndex, ResultEntity entity) {
		List<ResultEntity> column = dataList.get(columnIndex);
		System.out.println("Column size: " + column.size());
		column.add(entity);

		if (column.size() > maxRowCount) maxRowCount = column.size();
	}

	public Object[][] getDataTable() {
		if (dataList == null || dataList.size() == 0) return null;

		Object[][] dataTable = new Object[maxRowCount][columnsCount];

		System.out.println("Column size init: " + maxRowCount);

		for (int i = 0; i < maxRowCount; i++) {
			for (int j = 0; j < columnsCount; j++) {
				if (dataList.get(j).size() > i) dataTable[i][j] = dataList.get(j).get(i).toString();
			}
		}

		return dataTable;
	}
}
