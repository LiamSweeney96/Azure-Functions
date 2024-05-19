package com.function;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;



public class TriggerFunction {

    // TimerTrigger function to analyse the database data every 5 seconds
    @FunctionName("TriggerFunction")
        public void run(
            @TimerTrigger(name = "timerInfo", schedule = "*/5 * * * * *") String timerInfo,
            final ExecutionContext context
        ) {
        
        context.getLogger().info("Database triggered: " + timerInfo);


        // String that will be used to connect to the database
        String connectionString = "jdbc:sqlserver://courseworkone.database.windows.net:1433;database=CourseworkDB;user=sc21l2s@courseworkone;password={abcd1234!};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
        
        // Query to get the minimum, maximum, and average from each sensor
        String sqlQuery = "SELECT MIN(Temperature) AS MinimumTemp, "
                        + "MAX(Temperature) AS MaximumTemp, " 
                        + "AVG(Temperature) AS AverageTemp, " 
                        + "MIN(Wind) AS MinimumWind, "
                        + "MAX(Wind) AS MaximumWind, "
                        + "AVG(Wind) AS AverageWind, " 
                        + "MIN(RHumidity) AS MinimumRHumidity, " 
                        + "MAX(RHumidity) AS MaximumRHumidity, "
                        + "AVG(RHumidity) AS AverageRHumidity, " 
                        + "MIN(CO2) AS MinimumCO2, "
                        + "MAX(CO2) AS MaximumCO2, "
                        + "AVG(CO2) AS AverageCO2 " 
                        + "FROM SensorData";
                        
        try {

            // Establish a connection using the connection string
            Connection conn = DriverManager.getConnection(connectionString);
            PreparedStatement statement = conn.prepareStatement(sqlQuery);

            // Get the set of results from the prepared query
            ResultSet results = statement.executeQuery();

            // If the result set is not empty, get the values for minimum, maximum, and average of each sensor
            if (results.next()) {
                double minimumTemp = results.getDouble("MinimumTemp");
                double maximumTemp = results.getDouble("MaximumTemp");
                double averageTemp = results.getDouble("AverageTemp");
                double minimumWind = results.getDouble("MinimumWind");
                double maximumWind = results.getDouble("MaximumWind");
                double averageWind = results.getDouble("AverageWind");
                double minimumRHumidity = results.getDouble("MinimumRHumidity");
                double maximumRHumidity = results.getDouble("MaximumRHumidity");
                double averageRHumidity = results.getDouble("AverageRHumidity");
                double minimumCO2 = results.getDouble("MinimumCO2");
                double maximumCO2 = results.getDouble("MaximumCO2");
                double averageCO2 = results.getDouble("AverageCO2");

                context.getLogger().info("Minimum Temperature: " + minimumTemp);
                context.getLogger().info("Maximum Temperature: " + maximumTemp);
                context.getLogger().info("Average Temperature: " + averageTemp);
                context.getLogger().info("Minimum Wind Speed: " + minimumWind);
                context.getLogger().info("Maximum Wind Speed: " + maximumWind);
                context.getLogger().info("Average Wind Speed: " + averageWind);
                context.getLogger().info("Minimum Relative Humidity: " + minimumRHumidity);
                context.getLogger().info("Maximum Relative Humidity: " + maximumRHumidity);
                context.getLogger().info("Average Relative Humidity: " + averageRHumidity);
                context.getLogger().info("Minimum CO2 Quantity: " + minimumCO2);
                context.getLogger().info("Maximum CO2 Quantity: " + maximumCO2);
                context.getLogger().info("Average CO2 Quantity: " + averageCO2);
            }

        }  catch (Exception e) {
            context.getLogger().severe("Exception: " + e.getMessage());
        }
    }
}
