package tdbms.StorageManager;

import tdbms.QueryProcessor.TDBMSException;
import tdbms.StorageManager.Controllers.IndexController;

import java.io.*;
import java.util.Random;


public class TestTraj {
    public static void main(String[] args) {
        String tname = "test";
        try {
            writeSequence(tname, 5000);
            IndexController ic = new IndexController(tname);
            long target = 20;
            long result = ic.getDataPositionById(target);
            long max;
            if (result < 0) {
                System.out.printf("Did not find key %d in index\n", target);
            } else {
                System.out.printf("Index %d is at position %d in the data file.\n", target, result);
            }
            max = ic.getMaxId();
            System.out.printf("Max index is %d\n", max);

            ic.addNewIndex(max + 1, 10000);
            max = ic.getMaxId();
            System.out.printf("Added new index %d with position %d\n", max, ic.getDataPositionById(max));
            ic.deleteIndex(target);
            ic.deleteIndex(target);
            target = 50;
            ic.deleteIndex(target);
            for (int i=10; i < 30; i++ ) {
                ic.deleteIndex(i);
            }
            target = 100;
            ic.deleteIndex(target);
            target = 133;
            ic.deleteIndex(target);
            result = ic.getDataPositionById(target);
            if (result < 0) {
                System.out.printf("Did not find key %d in index\n", target);
            } else {
                System.out.printf("Index %d is at position %d in the data file.\n", target, result);
            }
        } catch (IOException e) { e.printStackTrace(); }
        catch (TDBMSException e) { e.printStackTrace(); }
    }

    public static void writeSequence(String tname, int size) throws IOException, TDBMSException {
        IndexController ic = new IndexController(tname);
        ic.eraseDatabase();
        int pos = 50;
        for (int i = 1; i <= size; i++) {
            Random rand = new Random();
            pos += rand.nextInt((150 - 10) + 1) + 10;
            ic.addNewIndex(i, pos);
        }
    }
}
