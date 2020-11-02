package paxos;

import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    // your data here
    public boolean ok;
    public Object value; // value which has received
    public long accepttime; // last time the proposal was accepted, the accepttime
    public long preptime;  // highest prepare seen
    public int maxDone;
    public int id;

    // Your constructor and methods here

    public Response() {

    }

    public Response(boolean ok, Object value,long preptime, long accepttime, int maxDone, int id) {
        this.ok = ok;
        this.value = value;
        this.preptime = preptime;
        this.accepttime = accepttime;
        this.maxDone = maxDone;
        this.id = id;
    }
}
