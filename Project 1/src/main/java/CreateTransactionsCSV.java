import java.io.*;
import java.io.FileWriter;
import java.io.IOException;

public class CreateTransactionsCSV {


    public String generateTransactionTotal(){
        float totalTrans = (float)(Math.random()*(1000.0-10.0+1)+10.0);
        return Float.toString(totalTrans);
    }


    public String generateRandomNumberOfItems(){
        int TN = (int)(Math.random()*(10-1+1)+1);
        return Integer.toString(TN);

    }


    public String generateRandomTransDescription(){
        String defaultString = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdedfghijklmnopqrstuvwxyz";

        int randomLength = (int)(Math.random()*(50-20+1)+20);

        StringBuilder sb = new StringBuilder(randomLength);

        for (int i = 0; i < randomLength; i++) {

            int index = (int)(defaultString.length() * Math.random());

            sb.append(defaultString.charAt(index));
        }

        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        BufferedWriter TW = new BufferedWriter(new FileWriter("D:\\Acadamics WPI\\CS585 Big Data Management\\Project 1\\Data\\Transactions.csv"));


        for(int i=0;i<5000000;i++){
           int customer_id = (i%50000) + 1;

            CreateTransactionsCSV ts = new CreateTransactionsCSV();
            String ID = Integer.toString(i+1);
            String customerID = Integer.toString(customer_id);
            String tt = ts.generateTransactionTotal();
            String nit = ts.generateRandomNumberOfItems();
            String td = ts.generateRandomTransDescription();


            String data = ID+","+customerID+","+tt+","+nit+","+td;
            TW.write(data);
            String newline ="\n";
            TW.write(newline);

        }
        TW.close();
    }
}
