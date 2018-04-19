/**
 * Created by bwolfson on 4/13/2018.
 */
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Generator {

    private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final String NEW_LINE_SEPARATOR = "\n";

    private static Customer generateRandomCustomer(int id){
        Random rand = new Random();
        // string length 10-20
        int strLen = rand.nextInt(11) + 10;
        String name = randomAlphaNumeric(strLen);
        // age 10-70
        int age = rand.nextInt(61) + 10;
        // country code 1-10
        int countryCode = rand.nextInt(10) + 1;
        double min = 100.0;
        double max = 10000.0;
        double salary = min + rand.nextDouble() * (max - min);
        // round salary to hundredths place
        salary = (double)Math.round(salary * 100d) / 100d;
        Customer c = new Customer(id, name, age, countryCode, salary);
        return c;
    }

    private static Transaction generateRandomTransaction(int transId, int custId){
        Random rand = new Random();
        double min = 10.0;
        double max = 1000.0;
        double transValue = min + rand.nextDouble() * (max - min);
        transValue = (double)Math.round(transValue * 100d) / 100d;
        int transNumItems = rand.nextInt(10) + 1;
        int strLen = rand.nextInt(31) + 20;
        String transDesc = randomAlphaNumeric(strLen);
        Transaction t = new Transaction(transId, custId, transValue, transNumItems, transDesc);
        return t;
    }

    //https://dzone.com/articles/generate-random-alpha-numeric
    private static String randomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        while (count-- != 0) {
            int character = (int)(Math.random()*ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }

    public static void main(String[] args){
    	/*
        List<Customer> customers = new ArrayList<Customer>();
        for(int i = 1; i < 50001; i++){
            Customer c = generateRandomCustomer(i);
            customers.add(c);
            System.out.println(c.toString());
        }
        //write customer csv file
        FileWriter fw = null;
        try{
            fw = new FileWriter("customers.csv");
            for(Customer c: customers){
                fw.append(c.toString());
                fw.append(NEW_LINE_SEPARATOR);
            }
        }catch (Exception e){
            System.out.println("Error in csv writing");
            e.printStackTrace();
        }finally {
            try{
                fw.flush();
                fw.close();
            }catch (IOException e){
                System.out.println("Error while flushing/closing");
                e.printStackTrace();
            }
        }
        */
    	FileWriter fw = null;
	    //List<Transaction> transactions = new ArrayList<Transaction>();
		try {
			//write transaction csv file
		    fw = new FileWriter("transactions.csv");
			for(int i = 0; i < 5000000; i += 100){
	            for(int j = 1; j < 101; j++){
					int custId = i/100 + 1;
					int transId = i + j;
					Transaction t = generateRandomTransaction(transId, custId);
					//transactions.add(t);
					fw.append(t.toString());
					fw.append(NEW_LINE_SEPARATOR);
	                System.out.println(t.toString());
			    }
			}
		}catch (Exception e){
		    System.out.println("Error in csv writing");
		    e.printStackTrace();
		}finally {
		    try{
		        fw.flush();
		        fw.close();
		    }catch (IOException e){
		        System.out.println("Error while flushing/closing");
		        e.printStackTrace();

		    }
		}
    }
}
