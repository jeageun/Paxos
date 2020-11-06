package paxos;


import java.io.Serializable;

public class Instance implements Serializable {
    public long preptime=-1;
    public long accepttime =-1;  //  if in phase 1, the acceptor replies ok
    public Object value;
    public State state = State.Pending;

    public Instance(long preptime, long accepttime, Object value, State state) {
        this.preptime = preptime;       // highest prep seen
        this.accepttime = accepttime;   // highest accept seen
        this.value = value;        // highest accept seen
        this.state = state;
    }

    public Instance() {

    }



}
