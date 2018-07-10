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

	public ResultQuery(int columnsCount) {
		this.columnsCount = columnsCount;

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
		column.add(entity);
	}

	public Object[][] getDataTable() {
		if (dataList == null || dataList.size() == 0) return null;

		Object[][] dataTable = new Object[columnsCount][dataList.get(0).size()];

		for (int i = 0; i < columnsCount; i++) {
			List<ResultEntity> columnResults = dataList.get(i);
			int columnResultsCount = columnResults.size();
			for (int j = 0;j < columnResultsCount; j++) {
				dataTable[i][j] =  columnResults.get(j).toString();
			}
		}

		return dataTable;
	}
}
