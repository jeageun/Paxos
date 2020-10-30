package paxos;

import java.io.Serializable;

public class Value implements Serializable {
    Object value;
    long preptime;
    long accepttime;
    State status;

    public Value(){
        this.value=null;
        this.preptime = -1;
        this.accepttime = -1;
        this.status = State.Pending;
    }
}