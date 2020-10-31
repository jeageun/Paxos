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

/*
    private void Proceed(Op operation){
        Op waiting;
        while(true){
            int serv = this.Serverseq;
            this.px.Start(serv,operation);
            waiting = this.wait(serv);
            if ()
        }
    }

 */

    // RMI handlers
    public Response Get(Request req) {
        // Your code here
        this.mutex.lock();
        try{
            Response res = new Response();
            Op operation = new Op("Get", req.seq, req.key, req.value);

            if(kvstore.containsKey(req.key))
            /*
            Response res = new Response();
            Op operation = new Op("Get", req.seq, req.key, req.value);
            Op waiting;
            this.px.Start(this.Serverseq, operation);
            while (true) {
                int serv = this.Serverseq;
                waiting = this.wait(serv);
                if (waiting.op == "Put") {
                    this.kvstore.put(waiting.key, waiting.value);
                }
                if (operation.ClientSeq == waiting.ClientSeq) {
                    if (waiting.op == "Get") {
                        res.value = this.kvstore.get(waiting.key);
                        res.ok = true;
                        break;
                    }
                }
                this.Serverseq++;
            }
            return res;

             */
        }finally{
            this.mutex.unlock();
        }
    }

    public Response Put(Request req){
        // Your code here
        this.mutex.lock();
        try {
            Op operation = new Op("Put", req.seq, req.key, req.value);
            Response res = new Response();
            Op waiting;

            while (true) {
                int serv = this.Serverseq;
                this.px.Start(this.Serverseq, operation);
                waiting = this.wait(serv);
                if (waiting.op == "Put") {
                    this.kvstore.put(waiting.key, waiting.value);
                }
                if (operation.ClientSeq == waiting.ClientSeq) {
                    if (waiting.op == "Put") {
                        res.ok = true;
                    } else {
                        res.ok = false;
                    }
                    break;
                }
                this.Serverseq++;
            }

            return res;
        }
        finally{
            this.mutex.unlock();
        }
    }


}
