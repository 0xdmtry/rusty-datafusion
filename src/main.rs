use datafusion::prelude::*;
mod csv_writer;


#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "trip_data",
        "../rusty_fusion/yellow_tripdata_2024-01.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    let queries = [
        ("01-avg_fare_amount_per_vendor.csv", r#"
            SELECT "VendorID",
                   AVG("fare_amount") AS "AvgFareAmount"
            FROM trip_data
            GROUP BY "VendorID"
        "#, "Average Fare Amount Per Vendor"),

        ("02-total_number_of_trips_per_vendor.csv", r#"
            SELECT "VendorID",
                   COUNT(*) AS "TotalTrips"
            FROM trip_data
            GROUP BY "VendorID"
        "#, "Total Number of Trips Per Vendor"),

        ("03-avg_trip_distance_by_rate_code.csv", r#"
            SELECT "RatecodeID",
                   AVG("trip_distance") AS "AvgTripDistance"
            FROM trip_data
            GROUP BY "RatecodeID"
        "#, "Average Trip Distance by Rate Code"),

        ("04-total_amount_by_payment_type.csv", r#"
            SELECT "payment_type",
                   SUM("total_amount") AS "TotalAmountCollected"
            FROM trip_data
            GROUP BY "payment_type"
        "#, "Total Amount by Payment Type"),

        ("05-avg_total_amount_by_payment_type.csv", r#"
            SELECT "passenger_count",
                   AVG("total_amount") AS "AvgTotalAmount"
            FROM trip_data
            GROUP BY "passenger_count"
        "#, "Average Total Amount by Payment Type"),

        ("06-trip_duration_analysis.csv", r#"
            SELECT "VendorID",
                   AVG((CAST("tpep_dropoff_datetime" AS DOUBLE) - CAST("tpep_pickup_datetime" AS DOUBLE)) / 60) AS "AvgTripDurationMinutes",
                   MIN((CAST("tpep_dropoff_datetime" AS DOUBLE) - CAST("tpep_pickup_datetime" AS DOUBLE)) / 60) AS "MinTripDurationMinutes",
                   MAX((CAST("tpep_dropoff_datetime" AS DOUBLE) - CAST("tpep_pickup_datetime" AS DOUBLE)) / 60) AS "MaxTripDurationMinutes"
            FROM trip_data
            GROUP BY "VendorID"
        "#, "Trip Duration Analysis"),

        ("07-revenue_by_time_of_the_day.csv", r#"
            SELECT CASE 
                       WHEN EXTRACT(HOUR FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)) BETWEEN 6 AND 11 THEN 'Morning'
                       WHEN EXTRACT(HOUR FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)) BETWEEN 12 AND 17 THEN 'Afternoon'
                       WHEN EXTRACT(HOUR FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)) BETWEEN 18 AND 23 THEN 'Evening'
                       ELSE 'Night'
                   END AS "TimeOfTheDay",
                   SUM("total_amount") AS "TotalRevenue"
            FROM trip_data
            GROUP BY "TimeOfTheDay"
            ORDER BY "TotalRevenue" DESC
        "#, "Revenue by Time of the Day"),

        ("08-peak_hours.csv", r#"
            SELECT EXTRACT(HOUR FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)) AS "Hour",
                   COUNT(*) AS "TotalTrips",
                   SUM("total_amount") AS "TotalRevenue"
            FROM trip_data
            GROUP BY "Hour"
            ORDER BY "TotalTrips" DESC
        "#, "Peak Hours"),

        ("09-trip_amount_by_payment_type.csv", r#"
            SELECT "payment_type",
                   AVG("tip_amount") AS "AvgTipAmount",
                   SUM("tip_amount") AS "TotalTipAmount",
                   COUNT(*) AS "TotalTrips"
            FROM trip_data
            GROUP BY "payment_type"
            ORDER BY "TotalTipAmount" DESC
        "#, "Trip Amount by Payment Type"),

        ("10-weekdays_vs_weekends_trip_count.csv", r#"
            SELECT CASE 
                       WHEN EXTRACT(DOW FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)) IN (0, 6) THEN 'Weekend'
                       ELSE 'Weekday'
                   END AS "DayType",
                   COUNT(*) AS "TotalTrips",
                   SUM("total_amount") AS "TotalRevenue"
            FROM trip_data
            GROUP BY "DayType"
            ORDER BY "TotalRevenue" DESC
        "#, "Weekdays vs Weekends Trip Count"),
    ];

    let directory_path = "../rusty_fusion/csv_files";

    for (file_name, query, description) in queries.iter() {
        println!("\n\n Executing: {}", description);
        let df = ctx.sql(query).await?;
        df.show().await?;

        if let Err(e) = csv_writer::write_to_csv(&ctx, query, directory_path, file_name).await {
            eprintln!("Failed to write CSV for {}: {}", description, e);
        } else {
            println!("CSV written successfully: {}/{}", directory_path, file_name);
        }
    }

    Ok(())
}
    