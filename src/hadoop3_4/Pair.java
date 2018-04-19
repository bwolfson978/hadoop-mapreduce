package hadoop3_4;

public class Pair {
	
	private int ccode;
	private int seen;
	
	public Pair(int ccode, int seen){
		this.ccode = ccode;
		this.seen = seen;
	}

	public int getCcode() {
		return ccode;
	}

	public int getSeen() {
		return seen;
	}
	
	public void setCcode(int ccode){
		this.ccode = ccode;
	}
	
	public void setSeen(int seen){
		this.seen = seen;
	}
	

}
