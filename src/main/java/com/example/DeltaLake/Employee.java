package com.example.DeltaLake;

import io.delta.standalone.data.RowRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;

import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Employee {
    private Integer id;
    private String firstName;
    private String middleName;
    private String lastName;
    private String gender;
    private String birthDate;
    private String ssn;
    private Integer salary;

    public static Employee map(Row data){
        Employee employee = new Employee();
        employee.setId(data.getAs("id"));
        employee.setFirstName(data.getAs("firstName"));
        employee.setMiddleName(data.getAs("middleName"));
        employee.setLastName(data.getAs("lastName"));
        employee.setGender(data.getAs("gender"));
        employee.setBirthDate(data.getAs("birthDate"));
        employee.setSsn(data.getAs("ssn"));
        employee.setSalary(data.getAs("salary"));
        return employee;
    }

    public static Employee map(RowRecord data) {
        Employee employee = new Employee();
        employee.setId(data.getInt("id"));
        employee.setFirstName(data.getString("firstName"));
        employee.setMiddleName(data.getString("middleName"));
        employee.setLastName(data.getString("lastName"));
        employee.setGender(data.getString("gender"));
        employee.setBirthDate(data.getString("birthDate"));
        employee.setSsn(data.getString("ssn"));
        employee.setSalary(data.getInt("salary"));
        return employee;
    }

    public static Employee map(GenericRecord data, Map<String, String> partitionValues) {
        Employee employee = new Employee();
        employee.setId(Integer.valueOf(partitionValues.get("id")));
        employee.setFirstName(data.get("firstName").toString());
        employee.setMiddleName(data.get("middleName").toString());
        employee.setLastName(data.get("lastName").toString());
        employee.setGender(partitionValues.get("gender"));
        employee.setBirthDate(data.get("birthDate").toString());
        employee.setSsn(data.get("ssn").toString());
        employee.setSalary(Integer.valueOf(data.get("salary").toString()));
        return employee;
    }
}
