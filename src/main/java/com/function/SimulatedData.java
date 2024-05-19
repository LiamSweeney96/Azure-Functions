package com.function;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Optional;
import java.util.Random;

/**
 * Azure Functions with HTTP Trigger.
 */
public class SimulatedData {
    /**
     * This function listens at endpoint "/api/HttpExample". Two ways to invoke it using "curl" command in bash:
     * 1. curl -d "HTTP Body" {your host}/api/HttpExample
     * 2. curl "{your host}/api/HttpExample?name=HTTP%20Query"
     */
    @FunctionName("SimulatedData")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.GET, HttpMethod.POST},
                authLevel = AuthorizationLevel.ANONYMOUS)
                HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");
        
        String connectionString = "jdbc:sqlserver://courseworkone.database.windows.net:1433;database=CourseworkDB;user=sc21l2s@courseworkone;password={abcd1234!};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";

        try {

            Connection conn = DriverManager.getConnection(connectionString);
            PreparedStatement deleteStatement = conn.prepareStatement("DELETE FROM SensorData");

            deleteStatement.executeUpdate();

            PreparedStatement preparedStatement = conn.prepareStatement("INSERT INTO SensorData (SensorID, Temperature, Wind, RHumidity, CO2) VALUES (?, ?, ?, ?, ?)");
            Random random = new Random();
            int sensorID = 0;
            double temp, wind, rHumidity, cDioxide;
            
            for (int i = 1; i < 21; i++) {
                sensorID = i;
                temp = 8 + (15 - 8) * random.nextDouble();
                wind = 15 + (25 - 15) * random.nextDouble();
                rHumidity = 40 + (70 - 40) * random.nextDouble();
                cDioxide = 500 + (1500 - 500) * random.nextDouble();
                
                preparedStatement.setInt(1, sensorID);
                preparedStatement.setDouble(2, temp);
                preparedStatement.setDouble(3, wind);
                preparedStatement.setDouble(4, rHumidity);
                preparedStatement.setDouble(5, cDioxide);

                preparedStatement.executeUpdate();
            }

            // 

            return request.createResponseBuilder(HttpStatus.OK).body("Data added.").build();
        }  catch (Exception e) {
            context.getLogger().severe("Exception: " + e.getMessage());
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR).body("Error executing query").build();
        }
    }
}
