package simpledb.server;

import simpledb.remote.*;
import java.rmi.registry.*;

public class Startup {
   public static void main(String args[]) throws Exception {
      // configure and initialize the database
      SimpleDB.init(args[0]);
      
      // create a registry specific for the simpledb.server on the default port
      Registry reg = LocateRegistry.createRegistry(1099);
      
      // and post the simpledb.server entry in it
      RemoteDriver d = new RemoteDriverImpl();
      reg.rebind("simpledb", d);
      
      System.out.println("database simpledb.server ready");
   }
}
