package polimi.deib.city_sensing_server.most_contacted_chart;

import java.util.Collection;

public class MostContactedChartResponse {

	private Collection<MostContactedChartCell> internationalContantactsIn;
	private Collection<MostContactedChartCell> internationalContantactsOut;
	private Collection<MostContactedChartCell> nationalContantactsIn;
	private Collection<MostContactedChartCell> nationalContantactsOut;
	
	public Collection<MostContactedChartCell> getInternationalContantactsIn() {
		return internationalContantactsIn;
	}
	public void setInternationalContantactsIn(
			Collection<MostContactedChartCell> internationalContantactsIn) {
		this.internationalContantactsIn = internationalContantactsIn;
	}
	public Collection<MostContactedChartCell> getInternationalContantactsOut() {
		return internationalContantactsOut;
	}
	public void setInternationalContantactsOut(
			Collection<MostContactedChartCell> internationalContantactsOut) {
		this.internationalContantactsOut = internationalContantactsOut;
	}
	public Collection<MostContactedChartCell> getNationalContantactsIn() {
		return nationalContantactsIn;
	}
	public void setNationalContantactsIn(
			Collection<MostContactedChartCell> nationalContantactsIn) {
		this.nationalContantactsIn = nationalContantactsIn;
	}
	public Collection<MostContactedChartCell> getNationalContantactsOut() {
		return nationalContantactsOut;
	}
	public void setNationalContantactsOut(
			Collection<MostContactedChartCell> nationalContantactsOut) {
		this.nationalContantactsOut = nationalContantactsOut;
	}
	
}
