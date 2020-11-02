package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the request message for each RMI call.
 * Hint: You may need the sequence number for each paxos instance and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 * Hint: Easier to make each variable public
 */
public class Request implements Serializable {
    static final long serialVersionUID=1L;
    // Your data here
    public int seq;
    public long preptime; // proposed number
    public Object value;
    public int id ;
    public int maxDone;

    // Your constructor and methods here

    public Request(int seq, long preptime, Object value, int id, int maxDone) {
        this.seq = seq;
        this.preptime = preptime;
        this.value = value;
        this.id = id;
        this.maxDone = maxDone;
    }
}
