import java.io.FileWriter;
import java.io.IOException;

import java.util.Random;

public class DB2 {
    public static void main(String[] args) {
        DB2 ob =new DB2();
        ob.generatedataset(7000000);
    }
    public static void generatedataset(int n){
        try{
            FileWriter myw = new FileWriter("DB2.txt");
            for(int i=0; i<n;i++)
            {
                Random random= new Random();

                int x= random.nextInt(10001-1)+1;
                int y= random.nextInt(10001-1)+1;
                int h= random.nextInt(21-1)+1;
                int w= random.nextInt(6-1)+1;

                myw.write(x+","+y+","+h+","+w+"\n");
            }
            myw.close();
            System.out.println("File Successfully Created.");



        }catch (IOException e){
            System.out.println("An Error Occured.");
            e.printStackTrace();
        }
    }
}
