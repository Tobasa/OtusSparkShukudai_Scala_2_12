package Models

import java.sql.Timestamp

case class Crime
(
	incident_number: String,
	offense_code: Int,
	offense_code_group: String,
	offense_description: String,
	district: String,
	reporting_area: Int,
	shooting: String,
	occured_on_date: Timestamp,
	year: Int,
	month: Int,
	dat_of_week: String,
	hour: Int,
	urc_part: String,
	street: String,
	latitude: Double,
	longitude: Double,
	location: String, //(Double, Double) -- doesn't work for csv
)