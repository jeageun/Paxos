package kvpaxos;

import org.junit.Test;
import static org.junit.Assert.*;
import java.util.Random;
import java.nio.charset.Charset;

/**
 * This is a subset of entire test cases
 * For your reference only.
 */
public class KVPaxosTest {


    public void check(Client ck, String key, Integer value){
        Integer v = ck.Get(key);
        assertTrue("Get(" + key + ")->" + v + ", expected " + value, v.equals(value));
    }

    @Test
    public void TestBasic(){
        final int npaxos = 5;
        String host = "127.0.0.1";
        String[] peers = new String[npaxos];
        int[] ports = new int[npaxos];

        Server[] kva = new Server[npaxos];
        for(int i = 0 ; i < npaxos; i++){
            ports[i] = 1100+i;
            peers[i] = host;
        }
        for(int i = 0; i < npaxos; i++){
            kva[i] = new Server(peers, ports, i);
        }

        Client ck = new Client(peers, ports);
        System.out.println("Test: Basic put/get ...");
        ck.Put("app", 6);
        check(ck, "app", 6);
        ck.Put("a", 70);
        check(ck,  "a", 70);

        System.out.println("... Passed");
    }

    @Test
    public void TestComplex(){
        final int npaxos = 5;
        String host = "127.0.0.1";
        String[] peers = new String[npaxos];
        int[] ports = new int[npaxos];

        Server[] kva = new Server[npaxos];
        for(int i = 0 ; i < npaxos; i++){
            ports[i] = 1100+i;
            peers[i] = host;
        }
        for(int i = 0; i < npaxos; i++){
            kva[i] = new Server(peers, ports, i);
        }

        Client ck = new Client(peers, ports);
        System.out.println("Test: Basic put/get ...");
        ck.Put("app", 6);
        check(ck, "app", 6);
        ck.Put("a", 70);
        check(ck, "a", 70);
        ck.Put("app",25);
        check(ck, "app", 25);
        ck.ports[0] = 0;
        ck.ports[1] = 0;
        ck.Put("app", 14);
        check(ck, "app", 14);
        ck.Put("a", 15);
        check(ck, "a", 15);
        ck.Put("app",16);
        check(ck, "app", 16);
        ck.ports[0] = 1100;
        ck.ports[1] = 1101;
        ck.Put("B", 15);
        check(ck, "B", 15);
        check(ck, "app", 16);
        check(ck, "a", 15);


        System.out.println("... Passed");

    }

    @Test
    public void TestKV(){
        final int npaxos = 5;
        String host = "127.0.0.1";
        String[] peers = new String[npaxos];
        int[] ports = new int[npaxos];

        Server[] kva = new Server[npaxos];
        for(int i = 0 ; i < npaxos; i++){
            ports[i] = 1100+i;
            peers[i] = host;
        }
        for(int i = 0; i < npaxos; i++){
            kva[i] = new Server(peers, ports, i);
        }

        Client[] ck = new Client[3];
        for(int i = 0; i < 3; i++){
            ck[i] = new Client(peers, ports);
        }
        System.out.println("Test: Basic put/get ...");
        ck[0].Put("app", 6);
        check(ck[0], "app", 6);
        ck[0].Put("a", 70);
        check(ck[0], "a", 70);
        ck[1].Put("app",25);
        check(ck[1], "app", 25);
        ck[0].ports[0] = 0;
        ck[0].Put("app", 14);
        check(ck[0], "app", 14);
        check(ck[1], "app", 14);
        ck[0].Put("a", 15);
        check(ck[0], "a", 15);
        ck[0].ports[0] = 1100;
        ck[1].Put("app",16);
        ck[0].Put("B", 15);
        check(ck[0], "B", 15);
        ck[1].Put("B", 25);
        check(ck[1], "B", 25);
        check(ck[1], "app", 16);
        System.out.println("... Passed");
    }

    @Test
    public void TestALOT(){
        Random r = new Random();
        String[] arr = new String[40];

        for (int i =0;i<arr.length;i++) {
            byte[] array = new byte[7]; // length is bounded by 7
            new Random().nextBytes(array);
            arr[i] =  new String(array, Charset.forName("UTF-8"));
        }

        final int npaxos = 10;
        final int nclient = 2;
        String host = "127.0.0.1";
        String[] peers = new String[npaxos];
        int[] ports = new int[npaxos];

        Server[] kva = new Server[npaxos];
        for(int i = 0 ; i < npaxos; i++){
            ports[i] = 1100+i;
            peers[i] = host;
        }
        for(int i = 0; i < npaxos; i++){
            kva[i] = new Server(peers, ports, i);
        }

        Client[] ck = new Client[nclient];
        for(int i = 0; i < nclient; i++){
            ck[i] = new Client(peers, ports);
        }
        System.out.println("Test: Basic put/get ...");
        for(int i =0;i<nclient;i++){
            int input = r.nextInt(200);
            int address = r.nextInt(200)%(arr.length);
            int pt = r.nextInt(ports.length-1);
            ck[i%nclient].ports[pt] = 0;
            ck[i%nclient].Put(arr[address],input);
            check(ck[i%nclient], arr[address], input);
            check(ck[(i+input)%nclient], arr[address], input);
            ck[i%nclient].ports[pt] = 1100+pt;
        }

        System.out.println("... Passed");
    }
}
