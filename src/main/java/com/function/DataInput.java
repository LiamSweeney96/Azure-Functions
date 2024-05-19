package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Random;



public class DataInput {
    
    // TimerTrigger function to input data into the database every 5 seconds
    @FunctionName("DataInput")
    public void run(
        @TimerTrigger(name = "timerInfo", schedule = "*/5 * * * * *") String timerInfo,
        final ExecutionContext context
    ) {
        context.getLogger().info("Timer triggered: " + timerInfo);
        
        // String that will be used to connect to the database
        String connectionString = "jdbc:sqlserver://courseworkone.database.windows.net:1433;database=CourseworkDB;user=sc21l2s@courseworkone;password={abcd1234!};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";

        try {

            // Attempt to connect to the database
            Connection conn = DriverManager.getConnection(connectionString);

            // Remove previous entries from the database
            PreparedStatement deleteStatement = conn.prepareStatement("DELETE FROM SensorData");
            deleteStatement.executeUpdate();

            // Prepare the SQL statement which will query the database
            PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO SensorData (SensorID, Temperature, Wind, RHumidity, CO2) VALUES (?, ?, ?, ?, ?)");
            
            // Declare a new random object that will be used to generate a random value in the given range
            Random random = new Random();

            // Variables to hold the sensor data
            int sensorID;
            double temp, wind, rHumidity, cDioxide;
            
            // Loop to generate sensor data and insert it into the database with a given sensor ID
            for (int i = 1; i < 21; i++) {
                sensorID = i;
                temp = 8 + (15 - 8) * random.nextDouble();
                wind = 15 + (25 - 15) * random.nextDouble();
                rHumidity = 40 + (70 - 40) * random.nextDouble();
                cDioxide = 500 + (1500 - 500) * random.nextDouble();
                
                // Set variables as values in the prepared SQL query
                preparedStatement.setInt(1, sensorID);
                preparedStatement.setDouble(2, temp);
                preparedStatement.setDouble(3, wind);
                preparedStatement.setDouble(4, rHumidity);
                preparedStatement.setDouble(5, cDioxide);

                // Execute the query
                preparedStatement.executeUpdate();
            }

            context.getLogger().info("Data Successfully Added");

        }  catch (Exception e) {
            context.getLogger().severe("Exception: " + e.getMessage());
        }
    }
}
