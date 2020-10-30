package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HashMap;
import java.lang.Math;


/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here

    int seq;
    Value value;
    int[] done;
    HashMap<Integer,Value> map;
    int threashold;



    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        this.done = new int[peers.length];
        this.map = new HashMap<Integer,Value>();
        // initialize array
        for(int i=0;i<this.done.length;i++){
            this.done[i] = -1;
        }
        this.threashold = ((peers.length+1) /2);
        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare"))
                callReply = stub.Prepare(req);
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
        // Your code here
        this.mutex.lock();
        this.seq = seq;
        this.value = new Value();
        this.value.value= value;
        Thread t1 = new Thread(this);
        this.mutex.unlock();
        t1.start();

    }

    @Override
    public void run(){
        //Your code here
        /*
        if(this.seq < this.Min()){
            return;
        }
        */
        while(true) {
            long time = choseN(this.seq);
            Request packet = new Request();
            packet.seq  = this.seq;
            packet.value = this.value.value;
            packet.time = time;
            Response ack = sendPrepare(packet);

            if (ack.ok){
                packet.time = ack.time;
                packet.value = ack.value;
                Response ackback = sendAccept(packet);
                if(ackback.ok){
                    if (sendDecide(packet).ok){
                        break;
                    }
                }
            }

        }
    }


    private long choseN(int seq){
    //choosen, unique and higher than anynseen so far
        this.mutex.lock();
        try{
            if(!this.map.containsKey(seq))
            {

                return System.nanoTime();
            }
            Value val = this.map.get(seq);
            long num = System.nanoTime();
            return Math.max(num,val.preptime) +1;
        }
        finally{
            this.mutex.unlock();
        }
    }

    private Response sendPrepare(Request req){
        int count = 0;
        long act_time =-1;
        boolean flag = true;
        Object acp_val = req.value;

        for (int p=0;p<this.peers.length;p++){
            Response ack;
            if(p == this.me){
                ack = Prepare(req);
            }else{
                ack = this.Call("Prepare",req,p);
            }
            if(ack != null && ack.ok){
                count++;
                if(ack.time > act_time){
                    act_time=ack.time;
                    acp_val =ack.value;
                    flag = false;
                }
            }
        }

        Response ack = new Response();
        if(flag){
            acp_val = req.value;
            act_time = req.time;
        }
        if(count>=this.threashold){
            ack.ok = true;
            ack.value=acp_val;
            ack.time = req.time;
        }else{
            ack.ok = false;
        }
        return ack;
    }

    private Response sendAccept(Request req){
        int count = 0;
        long act_time =req.time;

        for (int p=0;p<this.peers.length;p++){
            Response ack;
            if(p == this.me){
                ack = Accept(req);
            }else{
                ack = this.Call("Accept",req,p);
            }
            if(ack != null && ack.ok){
                count++;
            }
        }

        Response ack = new Response();
        if(count>=this.threashold){
            ack.ok = true;
        }else{
            ack.ok = false;
        }
        return ack;
    }

    private Response sendDecide(Request req){
        int count = 0;
        long act_time =req.time;

        for (int p=0;p<this.peers.length;p++){
            Response ack;
            if(p == this.me){
                ack = Decide(req);
            }else{
                ack = this.Call("Decide",req,p);
            }
            if(ack != null && ack.ok){
                count++;
            }
        }

        Response ack = new Response();
        ack.ok = true;
        return ack;
    }



    // RMI handler
    public Response Prepare(Request req){
        // your code here
        this.mutex.lock();
        try{
            Response ack = new Response();
            // If it's the first trial
            if(!this.map.containsKey(req.seq)){
                Value val = new Value();
                val.preptime = req.time;
                val.value = req.value;
                this.map.put(req.seq,val);
                ack.time = val.accepttime;
                ack.value = val.value;
                ack.ok = true;
                return ack;
            }
            //else
            Value val = this.map.get(req.seq);

            if(req.time > val.preptime){
                val.preptime = req.time;
                ack.time = val.accepttime;
                ack.value = val.value;
                ack.ok = true;
            }
            else
            {
                ack.ok = false;
            }
            return ack;
        }finally{
            this.mutex.unlock();
        }
    }

    public Response Accept(Request req){
        // your code here
        this.mutex.lock();
        try{
            Response ack = new Response();
            if(!this.map.containsKey(req.seq)){
                Value val = new Value();
                val.preptime = req.time;
                val.accepttime = req.time;
                val.value = req.value;
                this.map.put(req.seq,val);
                ack.value = val.value;
                ack.ok = true;
                return ack;
            }
            Value val = this.map.get(req.seq);

            if(req.time >= val.preptime){
                val.preptime = req.time;
                val.accepttime = req.time;
                val.value = req.value;
                ack.value = val.value;
                ack.ok = true;
            }
            else
            {
                ack.ok = false;
            }
            return ack;
        }finally{
            this.mutex.unlock();
        }

    }

    public Response Decide(Request req){
        // your code here
        this.mutex.lock();
        try{
            Response ack = new Response();
            if(!this.map.containsKey(req.seq)){
                Value val = new Value();
                val.status = State.Decided;
                val.preptime = req.time;
                val.accepttime = req.time;
                val.value = req.value;
                this.map.put(req.seq,val);
                ack.ok = true;
                return ack;
            }
            Value val = this.map.get(req.seq);
            val.status = State.Decided;
            val.preptime = req.time;
            val.accepttime = req.time;
            val.value = req.value;
            ack.ok = true;
            return ack;
        }finally{
            this.mutex.unlock();
        }
    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        this.mutex.lock();
        try{
            if(seq>this.done[this.me]){
                this.done[this.me] = seq;
            }
        }finally{
            this.mutex.unlock();
        }
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        this.mutex.lock();
        try{
            int max = -1;
            for(Integer seq : this.map.keySet()){
                if(seq>max){
                    max = seq;
                }
            }
            return max;
        }finally{
            this.mutex.unlock();
        }
    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

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
    public int Min(){
        // Your code here
        this.mutex.lock();
        int min = Integer.MAX_VALUE;
        try{
          for(int i=0;i<peers.length;i++){
            if (this.done[i]<min)
            {
              min = this.done[i];
            }
          }
          return (min+1);
        }finally{
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
    public retStatus Status(int seq){
        // Your code here
        this.mutex.lock();

        try{
            retStatus ret;
            if(seq < this.Min()){
                if(!this.map.containsKey(seq)){
                    ret = new retStatus(State.Forgotten,null);
                    return ret;
                }
                Value v = this.map.get(seq);
                ret = new retStatus(v.status,v.value);
                return ret;
            }
            if(this.map.containsKey(seq)){
                Value v = this.map.get(seq);
                ret = new retStatus(v.status,v.value);
            }else{
                ret = new retStatus(State.Pending,null);
            }
            return ret;
        }finally
        {
            this.mutex.unlock();
        }
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }



}

