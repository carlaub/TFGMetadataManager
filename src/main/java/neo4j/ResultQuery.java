package neo4j;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 09/07/2018.
 *
 */

public class ResultQuery implements Serializable{
	List<String> columnsName;
	int columnsCount;
	List<List<ResultEntity>> dataList;
	int maxRowCount;

	public ResultQuery(List<String> columnsName) {
		this.columnsCount = columnsName.size();
		this.maxRowCount = 0;

		dataList = new ArrayList<>();
		this.columnsName = columnsName;

		for (int i = 0; i < columnsCount; i++) {
			dataList.add(new ArrayList<ResultEntity>());
		}
	}


	public int getColumnsCount() {
		return columnsCount;
	}

	public void setColumnsCount(int columnsCount) {
		this.columnsCount = columnsCount;
	}


	public List<String> getColumnsName() {
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


	public void addEntity(int columnIndex, ResultEntity entity) {
		List<ResultEntity> column = dataList.get(columnIndex);
		column.add(entity);

		if (column.size() > maxRowCount) maxRowCount = column.size();
	}

	public Object[][] getDataTable() {
		if (dataList == null || dataList.size() == 0) return null;

		Object[][] dataTable = new Object[maxRowCount][columnsCount];

		for (int i = 0; i < maxRowCount; i++) {
			for (int j = 0; j < columnsCount; j++) {
				if (dataList.get(j).size() > i) dataTable[i][j] = dataList.get(j).get(i).toString();
			}
		}

		return dataTable;
	}
}
