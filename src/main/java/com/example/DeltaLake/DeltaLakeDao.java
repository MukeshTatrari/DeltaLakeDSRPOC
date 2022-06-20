package com.example.DeltaLake;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.expressions.EqualTo;
import io.delta.standalone.expressions.Literal;
import io.delta.standalone.types.StructType;
import io.delta.tables.DeltaTable;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.spark.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.springframework.util.ObjectUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class DeltaLakeDao {

    @Autowired
    private SparkSession spark;

    @Autowired
    private DeltaLog deltaLog;

    public List<Employee> getAllEmployees() {
        Dataset<Row> df = spark.read().format("delta").load(Constants.empTableName);
        return df.collectAsList().stream().map(Employee::map).collect(Collectors.toList());

    }

    public Employee findEmployeeById(Integer employeeId) {
        List<Employee> employees = spark.read().format("delta").load(Constants.empTableName)
                .filter(functions.col("id").equalTo(employeeId))
                .collectAsList().stream().map(Employee::map).collect(Collectors.toList());
        ;
        if (!ObjectUtils.isEmpty(employees)) {
            return employees.get(0);
        } else {
            throw new RuntimeException("Employee with " + employeeId + " is not found ::");
        }
    }


    public List<Employee> getAllEmployeesDSR() {
        Snapshot latestSnapshot = deltaLog.update();
        List<Employee> employeeList = new ArrayList<Employee>();
        latestSnapshot.open().forEachRemaining(data -> {
            employeeList.add(Employee.map(data));
        });
        return employeeList;
    }


    public Employee getEmployeeDSR(Integer employeeId)  {
        Snapshot latestSnapshot = deltaLog.update();
        StructType schema = latestSnapshot.getMetadata().getSchema();
        List<Employee> employees = new ArrayList<>();
        DeltaScan scan = latestSnapshot.scan(
                new EqualTo(schema.column("id"), Literal.of(employeeId))
        );

        scan.getFiles().forEachRemaining(file -> {
            try {
                Path path = new Path(Constants.ABSS_FILE_PATH + file.getPath());
                AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>();
                ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(readSupport, path)
                        .set("parquet.avro.readInt96AsFixed", "true")
                        .build();
                GenericRecord nextRecord = reader.read();
                Employee e = Employee.map(nextRecord, file.getPartitionValues());
                employees.add(e);
            } catch (IOException e) {
                System.out.println("exception:::: " + e);
                e.printStackTrace();
            }
        });


        if (!ObjectUtils.isEmpty(employees)) {
            return employees.get(0);
        } else {
            throw new RuntimeException("Employee with " + employeeId + " is not found ::");
        }
    }

    public Employee updateEmployee(Integer employeeId, Employee employee) {
        DeltaTable deltaTable = DeltaTable.forPath(spark, Constants.empTableName);
        deltaTable.update(functions.column("id").equalTo(employeeId),
                new HashMap<String, Column>() {{
                    put("firstName", functions.lit(employee.getFirstName()));
                    put("middleName", functions.lit(employee.getMiddleName()));
                    put("lastName", functions.lit(employee.getLastName()));
                    put("gender", functions.lit(employee.getGender()));
                    put("ssn", functions.lit(employee.getSsn()));
                    put("salary", functions.lit(employee.getSalary()));
                    put("birthDate", functions.lit(employee.getBirthDate()));
                }});

        return findEmployeeById(employee.getId());
    }

    public void deleteEmployee(Integer employeeId) {
        DeltaTable deltaTable = DeltaTable.forPath(spark, Constants.empTableName);
        deltaTable.delete(functions.column("id").equalTo(employeeId));
    }


    public Employee createEmployee(Employee employee) {

        List<Employee> employeeList = new ArrayList<>();
        employeeList.add(employee);
        Encoder<Employee> employeeEncoder = Encoders.bean(Employee.class);
        Dataset<Row> empDF = spark.createDataset(employeeList, employeeEncoder).toDF();
        empDF.write().format("delta").mode("append").save(Constants.empTableName);
        List<Employee> employees = empDF.collectAsList().stream().map(Employee::map).collect(Collectors.toList());
        return findEmployeeById(employee.getId());
    }
}
