import requests
import csv
from datetime import datetime, timedelta

NON_DATA_ATTR = ["Country/Region","Province/State", "Lat", "Long"]
DATA_ATTR = ['Date', 'Type', 'Cases', "Cumulative Cases"]

class CovidDataPuller:
    def __init__(self, urls, output="melted_covid_cases.csv"):
        self.urls = urls
        self.output = output

    def get_and_parse_csvs(self):
        total_parsed_array = []
        for itemLabel in self.urls:
            response = requests.get(self.urls[itemLabel])
            if response.status_code == 200:
                print(f"Successfully downloaded \"{itemLabel}\" file from: {self.urls[itemLabel]}")
                _parsed_csv_dict_array = self.parse_csv(response, itemLabel)
                total_parsed_array += _parsed_csv_dict_array
        return total_parsed_array

    def get_non_data_values(self, row):
        _dict = {}
        for _attr in NON_DATA_ATTR:
            _dict[_attr] = row[_attr]
        return _dict

    def parse_date_string(self, date_str):
        date_elems = date_str.split("/")
        date_object = datetime(int(f"20{date_elems[2]}"), int(date_elems[0]), int(date_elems[1]))
        return date_object

    def date_to_string(self, date_obj):
        return f"{date_obj.month}/{date_obj.day}/{str(date_obj.year)[2:]}"

    def parse_csv(self, response, label):
        csv_reader = csv.DictReader(response.text.strip().split('\n'))
        melted_data = []
        for row in csv_reader:
            for _column in row:
                if _column not in NON_DATA_ATTR:
                    _melted_row = self.get_non_data_values(row)
                    _melted_row["Date"] = _column
                    row_date_obj = self.parse_date_string(_melted_row["Date"])
                    day_before_row_date_obj = row_date_obj - timedelta(days=1)
                    day_before_string = self.date_to_string(day_before_row_date_obj)
                    if day_before_string not in row:
                        _melted_row["Type"] = label
                        _melted_row["Cases"] = row[_column]
                        _melted_row["Cumulative Cases"] = row[_column]
                    else:
                        _melted_row["Type"] = label
                        _melted_row["Cases"] = int(row[_column]) - int(row[day_before_string])
                        _melted_row["Cumulative Cases"] = row[_column]
                    melted_data.append(_melted_row)
        return melted_data

    def write_csv(self, array_of_dicts):
        with open(self.output, 'w') as output_csv_file:
            fieldnames = NON_DATA_ATTR + DATA_ATTR
            writer = csv.DictWriter(output_csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for row_dict in array_of_dicts:
                writer.writerow(row_dict)



global_urls = {"Confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv", \
        "Deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"}

covidGlobalData = CovidDataPuller(global_urls, "time_series_covid_global.csv")
parsed_csv_dict_1 = covidGlobalData.get_and_parse_csvs()
covidGlobalData.write_csv(parsed_csv_dict_1)

unified_urls = {
    "Confirmed": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Confirmed.csv", \
    "Deaths": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Deaths.csv", \
    "Recovered": "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_19-covid-Recovered.csv" }

covidUnifiedData = CovidDataPuller(unified_urls, "time_series_covid.csv")
parsed_csv_dict_2 = covidUnifiedData.get_and_parse_csvs()
covidUnifiedData.write_csv(parsed_csv_dict_2)



