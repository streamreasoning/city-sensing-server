package polimi.deib.city_sensing_server.most_contacted_chart;

import java.util.Collection;

public class MostContactedChartResponse {

	private Collection<MostContactedChartCell> contactsChart;

	public Collection<MostContactedChartCell> getContactsChart() {
		return contactsChart;
	}

	public void setContactsChart(Collection<MostContactedChartCell> contactsChart) {
		this.contactsChart = contactsChart;
	}
		
}
