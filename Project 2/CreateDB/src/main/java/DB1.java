import java.io.FileWriter;
import java.io.IOException;

import java.util.Random;

public class DB1 {
    public static void main(String[] args) {
        DB1 ob =new DB1();
        ob.generatedataset(11000000);
    }
    public static void generatedataset(int n){
        try{
            FileWriter myw = new FileWriter("DB1.txt");
            for(int i=0; i<n;i++)
            {
                Random random= new Random();

                int x= random.nextInt(10001-1)+1;
                int y= random.nextInt(10001-1)+1;

                myw.write(+x+","+y+"\n");
            }
            myw.close();
            System.out.println("File Successfully Created.");



        }catch (IOException e){
            System.out.println("An Error Occured.");
            e.printStackTrace();
        }
    }
}
