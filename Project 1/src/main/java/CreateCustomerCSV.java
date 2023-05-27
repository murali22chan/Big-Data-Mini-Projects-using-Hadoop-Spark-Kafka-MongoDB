import java.io.*;
import java.io.FileWriter;
import java.io.IOException;

public class CreateCustomerCSV {

    public String generateRandomNames() {
        String defaultString = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdedfghijklmnopqrstuvwxyz";

        int randomLength = (int) (Math.random() * (20 - 10 + 1) + 10);

        StringBuilder sb = new StringBuilder(randomLength);

        for (int i = 0; i < randomLength; i++) {

            int index = (int) (defaultString.length() * Math.random());

            sb.append(defaultString.charAt(index));
        }

        return sb.toString();
    }


    public String generateRandomAge() {
        int randAge = (int) (Math.random() * (70 - 10 + 1) + 10);
        return Integer.toString(randAge);

    }

    public String generateGender() {
        int randNumValue = (int) (Math.random() * (2 - 1 + 1) + 1);

        if (randNumValue == 1) {
            return "Male";
        } else {
            return "Female";
        }

    }


    public String generateRandomCountryCode() {
        int CC = (int) (Math.random() * (10 - 1 + 1) + 1);
        return Integer.toString(CC);

    }


    public String generateRandomSalaryValues() {
        float salValue = (float) (Math.random() * (10000.0 - 100.0 + 1) + 100.0);
        return Float.toString(salValue);

    }



    public static void main(String[] args) throws IOException {
        // Iteration for ID
        BufferedWriter CDwriter = new BufferedWriter(new FileWriter("D:\\Acadamics WPI\\CS585 Big Data Management\\Project 1\\Data\\Customers.csv"));
        for (int k = 1; k <= 50000; k++) {
            CreateCustomerCSV t = new CreateCustomerCSV();

            String id = Integer.toString(k);
            String name = t.generateRandomNames();
            String age = t.generateRandomAge();
            String gender = t.generateGender();
            String cc = t.generateRandomCountryCode();
            String salary = t.generateRandomSalaryValues();

            String data = id+","+name+","+age+","+gender+","+cc+","+salary;
            CDwriter.write(data);
            String newline ="\n";
            CDwriter.write(newline);

        }
        CDwriter.close();
    }
}
