/**
 * Created by bwolfson on 4/13/2018.
 */
public class Customer {
    private int id;
    private String name;
    private int age;
    private int countryCode;
    private double salary;

    public Customer(int id, String name, int age, int countryCode, double salary){
        this.id = id;
        this.name = name;
        this.age = age;
        this.countryCode = countryCode;
        this.salary = salary;
    }

    public String toString(){
        String s = "";
        return String.valueOf(this.id) + "," +
                name + String.valueOf(this.age) + "," +
                String.valueOf(this.countryCode) + "," +
                String.valueOf(this.salary);
    }
}
