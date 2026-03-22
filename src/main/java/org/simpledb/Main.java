package org.simpledb;


import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException {
        String dbPath = "./mydb";

        System.out.println("=== Creating Database ===");
        SimpleDB db = new SimpleDB(dbPath);

        // ── Basic CRUD ──
        System.out.println("\n--- Basic Operations ---");
        db.put("name", "Alice");
        db.put("age", "30");
        db.put("city", "Seattle");

        System.out.println("name = " + db.get("name"));   // Alice
        System.out.println("age  = " + db.get("age"));    // 30
        System.out.println("city = " + db.get("city"));   // Seattle

        // ── Update ──
        db.put("age", "31");
        System.out.println("age  = " + db.get("age"));    // 31

        // ── Delete ──
        db.delete("city");
        System.out.println("city = " + db.get("city"));   // null

        // ── Transaction that COMMITS ──
        System.out.println("\n--- Transaction: Commit ---");
        db.begin();
        db.put("balance", "1000");
        db.put("status", "active");
        db.commit();
        System.out.println("balance = " + db.get("balance")); // 1000
        System.out.println("status  = " + db.get("status"));  // active

        // ── Transaction that ABORTS ──
        System.out.println("\n--- Transaction: Abort ---");
        db.begin();
        db.put("balance", "0");       // would set balance to 0
        db.put("status", "frozen");  // would set status to frozen
        System.out.println("(before abort) balance = " + db.get("balance"));
        db.abort(); // ROLLBACK! Both changes are undone
        System.out.println("(after abort)  balance = " + db.get("balance")); // 1000
        System.out.println("(after abort)  status  = " + db.get("status"));  // active

        // ── Persistence ──
        System.out.println("\n--- Persistence ---");
        db.close();
        System.out.println("Database closed. Reopening...");

        SimpleDB db2 = new SimpleDB(dbPath);
        System.out.println("name    = " + db2.get("name"));    // Alice
        System.out.println("balance = " + db2.get("balance")); // 1000
        System.out.println("status  = " + db2.get("status"));  // active

        // ── Crash Recovery ──
        System.out.println("\n--- Crash Recovery Demo ---");
        db2.begin();
        db2.put("balance", "9999"); // uncommitted write!
        System.out.println("Simulating crash (not calling commit)...");
        // db2.commit(); ← we intentionally skip this
        db2.close();

        System.out.println("Reopening and recovering...");
        SimpleDB db3 = new SimpleDB(dbPath);
        db3.recover();
        System.out.println("balance = " + db3.get("balance")); // 1000 (not 9999!)
        System.out.println("The uncommitted write was correctly discarded!");
        db3.close();

        System.out.println("\n=== Done ===");
    }
}