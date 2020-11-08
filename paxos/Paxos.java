package paxos;

import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable {

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here

    long time; // proposed number
    int threshold;
    Map<Integer, Instance> map = new HashMap<>(); // <seq, Instance> instances this server knows after receives the Prepare Message
    Queue<Map.Entry<Integer, Instance>> seqval = new ArrayDeque<>(); // proposals this server has
    int[] done;


    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports) {

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        this.time = this.me;
        threshold = (peers.length + 1) / 2;
        done = new int[peers.length];
        Arrays.fill(done, -1);


        // register peers, do not modify this part
        try {
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     * <p>
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     * <p>
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id) {
        Response callReply = null;

        PaxosRMI stub;
        try {
//            System.out.println("ID=" +me +", and ports[ID="+id+"]");
            Registry registry = LocateRegistry.getRegistry(this.ports[id]);
            stub = (PaxosRMI) registry.lookup("Paxos");
            if (rmi.equals("Prepare")) {
                callReply = stub.Prepare(req);
            } else if (rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if (rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch (Exception e) {
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     * <p>
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     * <p>
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value) {
        // Your code here
        this.mutex.lock();
        try {
            if (seq >= this.Min() && (!map.containsKey(seq))) { // || map.get(seq).state!= State.Decided)
                Instance inst = new Instance(-1, -1, value, State.Pending);
                seqval.add(new HashMap.SimpleEntry<>(seq, inst));

                map.put(seq, inst);   // new added.....
            }
        } finally {
            this.mutex.unlock();
        }
        Thread thread = new Thread(this);
        thread.start();

    }

    private long chooseN(long seq, long rejPreptime) {
        this.mutex.lock();
        try {
            long num = this.map.containsKey(seq) ? Math.max(this.map.get(seq).preptime, rejPreptime) : rejPreptime;
            while (time <= num) {
                time += peers.length;
            }
            return time;
        } finally {
            this.mutex.unlock();
        }
    }

    @Override
    public void run() {
        //Your code here
        Map.Entry<Integer, Instance> each;
        this.mutex.lock();
        try {
            each = seqval.poll();
        } finally {
            this.mutex.unlock();
        }
        if (each == null) {
            return;
        }
        Instance inst = each.getValue();
        int seq = each.getKey();
//        while(true){
        while (inst.state != State.Decided) {

            long preptime = chooseN(seq, inst.preptime);
            Request req = new Request(seq, preptime, inst.value, this.me, done[this.me]);
            Response ack = sendPrepare(req);

            if (ack.ok) {


                req.value = ack.value;
                req.preptime = ack.preptime;     // changed to :req.preptime = ack.preptime;

                req.maxDone = done[this.me];
                Response ackback = sendAccept(req);
                if (ackback.ok) {
//                    System.out.println("=======================");
//                    System.out.println("Paxas[ID=" + me+"] has succeeded collected majority of accept, seq=" +seq+", value = "+ackback.value);
//                    System.out.println("so now， req.preptime =" +req.preptime+", and req.value=" +req.value);
//                    System.out.println("=======================");
                    req.maxDone = done[this.me];
                    Response decision = sendDecide(req);
                    if (decision.ok) {
                        inst.preptime = req.preptime;
                        inst.accepttime = req.preptime;
                        inst.state = State.Decided;
                        inst.value = req.value;
//                        System.out.println("Paxas[ID=" + me + "] has succeeded proposal seq=" + seq + ", value = " + inst.value);
                    }else if(!decision.ok && decision.accepttime== Integer.MIN_VALUE){
                        inst.value = decision.value;   // rejection due to proposed value is different from decided one
                        inst.preptime= decision.preptime;
                    }

                } else {
//                    System.out.println("Paxas[ID=" + me+"] was rejected when collect majority of accept, seq=" +seq);
                    inst.preptime = ackback.preptime;
                    if (ackback.accepttime==Integer.MIN_VALUE){  //if proposed value is not equal to decided one
                        inst.value = ackback.value;
                    }
                    continue;
                }
            } else {
                // Twos cases of rejection: 1. n <np    2. proposed 的value !=decided value
                inst.preptime = ack.preptime;
                if(ack.accepttime== Integer.MIN_VALUE){ // if already DECIDED, change to decided value
                    inst.value = ack.value;
//                    System.out.println("Paxas[ID=" + me+"] was rejected at proposal seq=" +seq+", rejected preptime ="+ack.preptime+", case 2");
//                    System.out.println("Thus now its value is changed to " +inst.value);

                }else{
//                    System.out.println("Paxas[ID=" + me+"] was rejected at proposal seq=" +seq+", rejected preptime ="+ack.preptime+", case 1");
                }

                continue;

            }
        }

//        }


    }

    private Response sendPrepare(Request req) {  // from sender side
        int count = 0;
        long act_time = -1;
        long pre_time = -1;
        Object act_val = null;
        Response prepare_ok;

        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < this.peers.length; i++) {
            prepare_ok = i == this.me ? Prepare(req) : this.Call("Prepare", req, i);

            if (prepare_ok != null) {

                this.done[i] = prepare_ok.maxDone;
                if (prepare_ok.ok) {
                    count++;
                    list.add(i);
                    if (prepare_ok.accepttime > act_time) {
                        act_time = prepare_ok.accepttime;
                        act_val = prepare_ok.value;
                    }
                }else if(!prepare_ok.ok && prepare_ok.accepttime == Integer.MIN_VALUE){ // when proposed value not equal to decided value
                    return new Response(false,prepare_ok.value,prepare_ok.preptime,Integer.MIN_VALUE,done[me],me);
                }
                if (prepare_ok.preptime > pre_time) {     // reject or not, send out the acceptor's highest prepare seen
                    pre_time = prepare_ok.preptime;
                }
            }
        }

        Response ack = new Response();
        if (count >= this.threshold) {
            ack.ok = true;
            ack.accepttime = act_time;
            ack.value = act_val == null ? req.value : act_val;
        } else {
            ack.ok = false;
        }

        ack.preptime = pre_time;     //  highest prepare seen from the Acceptors
//        if (ack.ok) {
//
//            System.out.println("Paxas[ID=" + me + "] has succeeded collected majority of prepare, seq=" + req.seq + ", value = " + ack.value + " with np=" + req.preptime);
//            System.out.println("prepare_ok paxos are" + list);
//
//        }
        return ack;
    }

    private Response sendAccept(Request req) {
        int count = 0;
        Response accept_ok = new Response();

        long highesPrepSeen = -1;
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < this.peers.length; i++) {
            accept_ok = i == this.me ? Accept(req) : Call("Accept", req, i);
            if (accept_ok != null) {
                this.done[i] = accept_ok.maxDone;
                if (accept_ok.ok) {
                    count++;

                    list.add(i);

                }else if(!accept_ok.ok && accept_ok.accepttime == Integer.MIN_VALUE){  // if proposed value not equal to decided one
                    return new Response(false,accept_ok.value,accept_ok.preptime,Integer.MIN_VALUE,done[me],me);
                }
                if (highesPrepSeen < accept_ok.preptime) {
                    highesPrepSeen = accept_ok.preptime;
                }
            }
        }
        Response ack = new Response();
        ack.ok = count >= this.threshold ? true : false;

        ack.preptime = highesPrepSeen;   // rejection or not , tell you the highestPrepSeen, so that you could start the next run
//        if (ack.ok) {
//            System.out.println("==============");
//            System.out.println("Paxas[ID=" + me + "] has succeeded collected majority of accept, seq=" + req.seq + ", value = " + req.value + ", with np = " + req.preptime);
//            System.out.println("accepted paxos are:" + list);
//            System.out.println("==============");
//        }
        return ack;
    }

    private Response sendDecide(Request req) {
        int count = 0;
        Response decide_ok;
        for (int i = 0; i < this.peers.length; i++) {
            decide_ok = i == this.me ? Decide(req) : Call("Decide", req, i);
            if (decide_ok != null) {

                this.done[i] = decide_ok.maxDone;
                if (decide_ok.ok) {
                    count++;
                }else if(!decide_ok.ok && decide_ok.accepttime==Integer.MIN_VALUE){
                    return new Response(false,decide_ok.value,decide_ok.preptime,Integer.MIN_VALUE,done[me],me);
                }
            }

        }
        Response ack = new Response();
//        ack.ok = count== peers.length? true: false; // 改了， some paxos may be deaf so you could not use count
        ack.ok = true;
        return ack;
    }

    // RMI handler      - at receiver side
    public Response Prepare(Request req) {
        // your code here
        this.mutex.lock();
        try {
            this.done[req.id] = req.maxDone; // yue

            Response prepare_ok;
            if (!this.map.containsKey(req.seq)) {
                Instance inst = new Instance(req.preptime, -1, null, State.Pending);    //
                this.map.put(req.seq, inst);
                prepare_ok = new Response(true, req.value, req.preptime, req.preptime, done[this.me], this.me);

            } else {

                // NOT DECIDED， or DEICDED and proposed value is the same
                // DECIDED,but proposed value is different，reject and let it start the next run
                Instance inst = this.map.get(req.seq);
                boolean b1 = inst.state != State.Decided;
                boolean b2 = inst.state == State.Decided;
                boolean b3 = inst.value == req.value;
                if(inst.state != State.Decided || (inst.state == State.Decided && inst.value .equals(req.value) )){
                    if (req.preptime > inst.preptime) {   // n > n_p , preptime > highest prepare seen
                        inst.preptime = req.preptime;    // update the highest proposal preptime seen
                        prepare_ok = new Response(true, inst.value, inst.preptime, inst.accepttime, done[this.me], this.me);

                    } else {
                        prepare_ok = new Response(false, inst.value, inst.preptime, inst.accepttime, done[this.me], this.me);
                    }
                }else{
                    prepare_ok = new Response(false, inst.value, inst.preptime, Integer.MIN_VALUE, done[this.me], this.me);
                }



            }

            return prepare_ok;

        } finally {
            this.mutex.unlock();
        }


    }

    public Response Accept(Request req) {
        // your code here
        this.mutex.lock();
        try {
            this.done[req.id] = req.maxDone;  //yue
            Response accept_ok;
            if (!map.containsKey(req.seq)) {
                System.out.println("Error, paxos [" + this.me + "]Receive accept request from paxos[" + req.id + "]before receive the prepare request, return null");
                return null;
            }
            Instance inst = map.get(req.seq);
            // NOT DECIDED， or DEICDED and proposed value is the same
            // DECIDED,but proposed value is different，reject and let it start the next run
            if(inst.state!= State.Decided || inst.value.equals(req.value)){
                if (req.preptime >= inst.preptime) {
                    inst.preptime = req.preptime;
                    inst.accepttime = req.preptime;
                    inst.value = req.value;
                    accept_ok = new Response(true, inst.value, inst.preptime, inst.accepttime, done[this.me], this.me);
                } else {
                    accept_ok = new Response(false, inst.value, inst.preptime, inst.accepttime, done[this.me], this.me);
                }
            }else{
                accept_ok = new Response(false,inst.value, inst.preptime, Integer.MIN_VALUE, done[this.me], this.me); // 用来表示case 2
            }

            return accept_ok;
        } finally {
            this.mutex.unlock();
        }

    }

    public Response Decide(Request req) {
        // your code here
        this.mutex.lock();
        try {
            this.done[req.id] = req.maxDone;  // yue
            Response decide_ok;
            if (!map.containsKey(req.seq)) {
                System.out.println("Paxos [" + this.me + "]Receive decide request from paxos[" + req.id + "]before receive the prepare request, return null");
                return null;
            }

            // NOT DECIDED， or DEICDED and proposed value is the same
            // DECIDED,but proposed value is different，reject and let it start the next run

            Instance inst = this.map.get(req.seq);

            if(inst.state!=State.Decided || inst.value.equals(req.value)){
                inst.value = req.value;
                inst.state = State.Decided;
                //inst.preptime= req.preptime;     //把这里uncomment之后呢
                // inst.accepttime = req.preptime;

//                System.out.println("Paxos[ID=" + req.id + "] -> Paxos[ID" + me + "] agree on" + req.value);


                decide_ok = new Response(true, inst.value, inst.preptime, inst.accepttime, this.done[this.me], this.me);
            }else {  // if propose a value which is different from the decided value
                decide_ok = new Response(false, inst.value, inst.preptime, Integer.MIN_VALUE, this.done[this.me], this.me);
            }


            return decide_ok;
        } finally {
            this.mutex.unlock();
        }

    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     * <p>
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        this.mutex.lock();
        try {
            if (seq > this.done[this.me]) {
//                System.out.println("Done called, this.done[me="+this.me +"] =" +this.done[this.me]);
                this.done[this.me] = seq;
//                System.out.println("made equal to seq =" +seq);
            }
        } finally {
            this.mutex.unlock();
        }
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max() {
        // Your code here
        this.mutex.lock();
        try {
            int max = -1;
            for (Integer seq : this.map.keySet()) {
                if (seq > max) {
                    max = seq;
                }
            }
            return max;
        } finally {
            this.mutex.unlock();
        }
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().
     * <p>
     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.
     * <p>
     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.
     * <p>
     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min() {
        // Your code here
        this.mutex.lock();
        try {
            int min = Integer.MAX_VALUE;
            for (int i = 0; i < peers.length; i++) {
                if (this.done[i] < min) {
                    min = this.done[i];
                }
            }

            for (int seq : map.keySet()) {   // free up memory
                if (seq < min) {
                    map.remove(seq);
                }
            }
            return (min + 1);
        } finally {
            this.mutex.unlock();
        }
    }


    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq) {
        // Your code here
        if (seq >= Min()) {
            if (map.containsKey(seq)) {
                Instance inst = map.get(seq);

                return new retStatus(inst.state, inst.value);
            } else {
                //System.out.println("Not a known instance on Paxos[ID="+this.me+"].");
                return new retStatus(State.Pending, null);
            }
        }
        return new retStatus(State.Forgotten, null);

    }

    /**
     * helper class for Status() return
     */
    public class retStatus {
        public State state;
        public Object v;

        public retStatus(State state, Object v) {
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill() {
        this.dead.getAndSet(true);
        if (this.registry != null) {
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch (Exception e) {
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead() {
        return this.dead.get();
    }

    public void setUnreliable() {
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable() {
        return this.unreliable.get();
    }


}
