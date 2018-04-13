/**
 * Created by bwolfson on 4/13/2018.
 */
public class Transaction {
    private int transId;
    private int custId;
    private double transValue;
    private int transNumItems;
    private String transDesc;

    public Transaction(int transId, int custId, double transValue, int transNumItems, String transDesc){
        this.transId = transId;
        this.custId = custId;
        this.transValue = transValue;
        this.transNumItems = transNumItems;
        this.transDesc = transDesc;
    }

    public String toString(){
        return String.valueOf(this.transId) + "," +
                String.valueOf(this.custId) + "," +
                String.valueOf(this.transValue) + "," +
                String.valueOf(transNumItems) + "," + transDesc;
    }
}
