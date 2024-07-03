package protobuf;


import java.io.File;
import java.io.FileInputStream;
// import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import com.google.protobuf.InvalidProtocolBufferException;

import protobuf.EmployeesOuterClass.*;


public class Main 
{
    public static void main(String[] args) throws IOException
    {
        System.out.println("Starting...");
    

        Employee e1 = Employee.newBuilder()
            .setId(1001)
            .setName("Thanh")
            .setSalary(2000001)
            .build();

        Employee e2 = Employee.newBuilder()
            .setId(1002)
            .setName("Khai")
            .setSalary(2000002)
            .build();

        Employee e3 = Employee.newBuilder()
            .setId(1003)
            .setName("Khanh")
            .setSalary(2000003)
            .build();

        Employee e4 = Employee.newBuilder()
            .setId(1004)
            .setName("Quy")
            .setSalary(2000004)
            .build();

        Employee e5 = Employee.newBuilder()
            .setId(1005)
            .setName("Son")
            .setSalary(2000005)
            .build();

        
        Employees company = Employees.newBuilder()
            .addEmployees(e1)
            .addEmployees(e2)
            .addEmployees(e3)
            .addEmployees(e4)
            .addEmployees(e5)
            .build();


        byte[] fileData = company.toByteArray();

        File outputFile = new File("./target/employees_bin.txt");

        try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            outputStream.write(fileData);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            FileInputStream inputStream = new FileInputStream(outputFile);
            fileData = inputStream.readAllBytes();
            inputStream.close();
            Employees deserializedCompany = Employees.parseFrom(fileData);
            
            // Access and print employee details
            // for (Employee employee : deserializedCompany.getEmployeesList()) {
            //   System.out.println("Employee ID: " + employee.getId());
            //   System.out.println("Employee Name: " + employee.getName());
            //   System.out.println("Employee Salary: " + employee.getSalary());
            //   System.out.println("-------------------------");
            // }
            for (Employee employee : deserializedCompany.getEmployeesList())
            {
                System.out.print(employee.toString());
                System.out.println("-------------------------");
            }
            
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace(); // For debugging purposes
            System.err.println("Error deserializing data: " + e.getMessage());
        }


        System.out.println("End!");

    }
}