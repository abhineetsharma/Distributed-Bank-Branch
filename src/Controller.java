import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;


public class Controller {

    static List<Bank.InitBranch.Branch> branches;

    public static void main(String[] args) {

        if (args.length > 1) {
            Bank.InitBranch.Builder initBranch = Bank.InitBranch.newBuilder();

            int balance = Integer.parseInt(args[0]);

            if(balance<=0){
                System.err.println("Balance cannot be less then 1");
                System.exit(1);
            }
            String fileName = args[1];
            FileProcessor fPro = new FileProcessor(fileName);
            String str = null;

            while ((str = fPro.readLine()) != null) {
                String[] sArr = str.split(" ");
                String name = sArr[0];
                String ip = sArr[1];
                int branchPort = Integer.parseInt(sArr[2]);

                Bank.InitBranch.Branch.Builder branchBuilder = Bank.InitBranch.Branch.newBuilder();
                branchBuilder.setName(name);
                branchBuilder.setIp(ip);
                branchBuilder.setPort(branchPort);

                Bank.InitBranch.Branch branch = branchBuilder.build();

                initBranch.addAllBranches(branch);
            }
            if(fPro.getCount()==0){
                System.err.println("No text in branch.txt");
                System.exit(1);
            }
            balance /= initBranch.getAllBranchesCount();
            initBranch.setBalance(balance);
            initBranch.build();

            //System.out.println(initBranch.toString());

            Bank.BranchMessage.Builder msgBuilder = Bank.BranchMessage.newBuilder();
            msgBuilder.setInitBranch(initBranch);
            Bank.BranchMessage msg = msgBuilder.build();

            branches = initBranch.getAllBranchesList();

            for (Bank.InitBranch.Branch branch : branches) {
                Socket socket = null;
                try {
                    Thread.sleep(1000);
                    socket = new Socket(branch.getIp(), branch.getPort());
                    msg.writeDelimitedTo(socket.getOutputStream());
                    //socket.shutdownOutput();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    if (socket != null)
                        try {
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                }
            }

            for (int i = 1; i <= 1000; i++) {

                snapShot(i);
            }
        }

    }

    public static void snapShot(int snapShotId) {
        //System.out.println("snapshot");
        Random rn = new Random();
        int randomBranchNumber = rn.nextInt(branches.size());

        Bank.InitBranch.Branch randomBranch = branches.get(randomBranchNumber);
        //System.out.println("SnapShot id " + snapShotId + " for branch " + randomBranch.getPort());

        Bank.InitSnapshot initSnapshot = Bank.InitSnapshot.newBuilder().setSnapshotId(snapShotId).build();

        Bank.BranchMessage.Builder msgBuilder = Bank.BranchMessage.newBuilder();
        Bank.BranchMessage msg = msgBuilder.setInitSnapshot(initSnapshot).build();
        Socket socket = null;
        try {
            Thread.sleep(800);
            socket = new Socket(randomBranch.getIp(), randomBranch.getPort());

            msg.writeDelimitedTo(socket.getOutputStream());

            //socket.shutdownOutput();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (socket != null)
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }


        try {
            Thread.sleep(8000);
            StringBuilder sbr = new StringBuilder();

            Bank.RetrieveSnapshot retrieveSnapshot = Bank.RetrieveSnapshot.newBuilder().setSnapshotId(snapShotId).build();
            msg = Bank.BranchMessage.newBuilder().setRetrieveSnapshot(retrieveSnapshot).build();

            sbr.append("snapshot_id: " + snapShotId + "\n");
            int sum = 0;
            for (Bank.InitBranch.Branch branch : branches) {
                Thread.sleep(500);
                int port = branch.getPort();
                String ip = branch.getIp();

                Socket socketRetrieveSnapshot = new Socket(ip, port);

                msg.writeDelimitedTo(socketRetrieveSnapshot.getOutputStream());
                //socketRetrieveSnapshot.shutdownOutput();

                InputStream is = socketRetrieveSnapshot.getInputStream();


                Bank.BranchMessage message = Bank.BranchMessage.parseDelimitedFrom(is);

                Bank.ReturnSnapshot.LocalSnapshot localSnapshot = message.getReturnSnapshot().getLocalSnapshot();
                sum += localSnapshot.getBalance();
                sbr.append(branch.getName() + ": " + localSnapshot.getBalance());

                int i = 0;


                List<String> br = new ArrayList<>();
                for (Bank.InitBranch.Branch b : branches)
                    if (!b.getName().equalsIgnoreCase(branch.getName()))
                        br.add(b.getName());

                for (int c : localSnapshot.getChannelStateList()) {
                    sbr.append(", " + br.get(i) + "->" + branch.getName() + ": " + c);
                    sum += c;
                    i++;
                }

                sbr.append("\n");


                socketRetrieveSnapshot.close();
            }
            System.out.println(sbr);
            //System.out.println(sum);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
