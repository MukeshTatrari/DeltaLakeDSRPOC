package com.example.DeltaLake;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("api/v1/Employees")
public class DeltaLakeController {

    @Autowired
    private DeltaLakeService service;

    @GetMapping("/")
    public List<Employee> getEmployees() {
        return service.getAllEmployees();
    }

    @GetMapping("/{employeeId}")
    public Employee getEmployee(@PathVariable Integer employeeId) {
        return service.findEmployeeById(employeeId);
    }

    @PostMapping("/create")
    public Employee create(@RequestBody Employee employee) {
        return service.createEmployee(employee);
    }

    @PutMapping("/{employeeId}")
    public Employee update(@PathVariable Integer employeeId, @RequestBody Employee employee) {
            return service.updateEmployee(employeeId, employee);
    }

    @DeleteMapping("/delete/{employeeId}")
    public void delete(@PathVariable Integer employeeId) {
        service.deleteEmployee(employeeId);
    }


}
