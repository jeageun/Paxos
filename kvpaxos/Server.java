package kvpaxos;
import paxos.Paxos;
import paxos.State;
// You are allowed to call Paxos.Status to check if agreement was made.

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements KVPaxosRMI {

    ReentrantLock mutex;
    Registry registry;
    Paxos px;
    int me;

    String[] servers;
    int[] ports;
    KVPaxosRMI stub;

    // Your definitions here
    HashMap<String,Integer> kvstore;
    int Serverseq;


    public Server(String[] servers, int[] ports, int me){
        this.me = me;
        this.servers = servers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.px = new Paxos(me, servers, ports);
        // Your initialization code here
        Serverseq = 0;
        this.kvstore = new HashMap<String,Integer>();




        try{
            System.setProperty("java.rmi.server.hostname", this.servers[this.me]);
            registry = LocateRegistry.getRegistry(this.ports[this.me]);
            stub = (KVPaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("KVPaxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    public Op wait(int seq){
        int to = 10;
        while(true){
            Paxos.retStatus ret = this.px.Status(seq);
            if(ret.state == State.Decided){
                return Op.class.cast(ret.v);
            }
            try{
                Thread.sleep(to);
            } catch (Exception e){
                e.printStackTrace();
            }
            if( to < 1000){
                to = to * 2;
            }
        }
    }


    private void Proceed(Op operation){
        Op waiting;
        do{
            int serv = this.Serverseq;
            this.px.Start(serv,operation);
            waiting = this.wait(serv);
            if(waiting.op.equals("Put")){
                this.kvstore.put(waiting.key,waiting.value);
            }
            this.px.Done(serv);
            this.Serverseq++;
        }while(operation.ClientSeq != waiting.ClientSeq);
    }


    // RMI handlers
    public Response Get(Request req) {
        // Your code here
        this.mutex.lock();
        try{
            Op operation = new Op("Get", req.seq, req.key, req.value);
            Proceed(operation);
            Response res = new Response(this.kvstore.get(operation.key),true);
            return res;
        }finally{
            this.mutex.unlock();
        }
    }

    public Response Put(Request req){
        // Your code here
        this.mutex.lock();
        try {
            Op operation = new Op("Put", req.seq, req.key, req.value);
            Proceed(operation);
            Response res = new Response(-1,true);
            return res;
        }
        finally{
            this.mutex.unlock();
        }
    }


}
