package it.polimi.deib.city_sensing_server.map;

import java.util.Collection;

public class MapResponse {
	
	Collection<MapCell> cells;

	public Collection<MapCell> getCells() {
		return cells;
	}

	public void setCells(Collection<MapCell> cells) {
		this.cells = cells;
	}
	
	public void addElementToList(MapCell mapCell){
		cells.add(mapCell);
	}
	
	public void addElementsToList(Collection<MapCell> mapCells){
		cells.addAll(mapCells);
	}

}
