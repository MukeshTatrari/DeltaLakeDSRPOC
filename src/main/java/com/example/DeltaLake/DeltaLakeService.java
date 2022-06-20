package com.example.DeltaLake;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DeltaLakeService {

    @Autowired
    private DeltaLakeDao deltaLakeDao;

    public List<Employee> getAllEmployees() {
        return deltaLakeDao.getAllEmployeesDSR();
    }

    public Employee findEmployeeById(Integer employeeId) {
        return deltaLakeDao.getEmployeeDSR(employeeId);
    }

    public Employee createEmployee(Employee employee) {
        return deltaLakeDao.createEmployee(employee);
    }

    public Employee updateEmployee(Integer employeeId , Employee employee) {
        return deltaLakeDao.updateEmployee(employeeId,employee);
    }

    public void deleteEmployee(Integer employeeId){
         deltaLakeDao.deleteEmployee(employeeId);
    }
}
