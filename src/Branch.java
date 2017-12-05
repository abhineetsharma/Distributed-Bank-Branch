
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Branch {

    private static boolean printFlag = false;

    private static int serverBranchPort;//branch server port number
    private static String serverBranchName;//branch name
    private static int initialBalance;//Initial balance
    private static volatile Integer balance;//balance of the branch

    private static Map<String, Bank.InitBranch.Branch> branchMap;// data structure to store all the branch
    private static ConcurrentMap<Integer, Bank.ReturnSnapshot.LocalSnapshot> localSnapshotMap;// save local snapshot data w.r.t snapshot id
    private static ConcurrentMap<Integer, Map<String, Channel>> channelLocalSnapshotMap;// save local channel state for incoming transfer while the marker flag is true for the channel for a snapshot id

    private static Lock lock = new ReentrantLock();
//    private static Lock lock2 = new ReentrantLock();//lock for thread


    public static void main(String[] args) {

        ServerSocket branchSocket = null;
        Socket clientSocket = null;

        try {
            if (args.length > 1) {
                serverBranchName = args[0];
                serverBranchPort = Integer.parseInt(args[1]);

                branchSocket = new ServerSocket(serverBranchPort);
                System.out.println("Branch Server Started");
                int msgCount = 0;

                while (true) {
                    clientSocket = branchSocket.accept();

                    InputStream is = clientSocket.getInputStream();

                    Bank.BranchMessage msg = Bank.BranchMessage.parseDelimitedFrom(is);

                    print("\n\n----------------------------------------");
                    print("====Message received count = " + (++msgCount));
                    print("----------------------------------------\n\n");
                    print(msg.toString());

                    //InitBranch
                    if (branchMap == null && msg.hasInitBranch() && msg.getInitBranch().getAllBranchesCount() > 0) {
                        print("----Initial Branch Message Start----");

                        branchMap = new ConcurrentHashMap<>();
                        localSnapshotMap = new ConcurrentHashMap<>();
                        channelLocalSnapshotMap = new ConcurrentHashMap<>();

                        balance = msg.getInitBranch().getBalance();
                        initialBalance = balance;

                        for (Bank.InitBranch.Branch b : msg.getInitBranch().getAllBranchesList()) {
                            branchMap.put(b.getName(), b);
                        }
                        print(branchMap);
                        print("----Initial Branch Message End----");

                        //starting the Transfer Thread
                        //have to wait till all branch server are started
                        Thread.sleep(2000);
                        transferThread.start();
                    }
                    //Transfer
                    else if (msg.hasTransfer()) {

                        print("----Received Transfer Start----");
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
                        String branchName = bufferedReader.readLine();
                        print(branchName);

                        try {
                            Bank.Transfer transfer = msg.getTransfer();
                            int amount = transfer.getMoney();
                            print(amount + " + " + balance + " = " + (balance + amount));
                            lock.lock();
                            balance += amount;
                            if (channelLocalSnapshotMap != null) {
                                for (int snapshotId : channelLocalSnapshotMap.keySet()) {
                                    Map<String, Channel> channelMap = channelLocalSnapshotMap.get(snapshotId);
                                    Channel channel = channelMap.get(branchName);
                                    channel.add(amount);
                                }
                            }
                            print(channelLocalSnapshotMap);
                            lock.unlock();
                        } catch (Exception ex) {
                            ex.printStackTrace();
                        } finally {

                        }
                        print("----Received Transfer End----");
                    }
                    //IntiSnapshot
                    else if (msg.hasInitSnapshot()) {

                        print("----Init Snapshot Start----");
                        Bank.InitSnapshot intiSnapShot = msg.getInitSnapshot();
                        int snapshotId = intiSnapShot.getSnapshotId();
                        print("SnapShot received : " + snapshotId);

                        //saving local branch state
                        try {
                            if (localSnapshotMap != null && !localSnapshotMap.containsKey(snapshotId)) {
                                lock.lock();
                                Bank.ReturnSnapshot.LocalSnapshot localSnapshot = Bank.ReturnSnapshot.LocalSnapshot.newBuilder()
                                        .setSnapshotId(snapshotId)
                                        .setBalance(balance).
                                                build();

                                //channels to be coded
                                localSnapshotMap.put(snapshotId, localSnapshot);

                                if (!channelLocalSnapshotMap.containsKey(snapshotId)) {
                                    Map<String, Channel> channelMap = new HashMap<>();
                                    for (Bank.InitBranch.Branch branch : branchMap.values()) {
                                        if (!branch.getName().equalsIgnoreCase(serverBranchName))
                                            channelMap.put(branch.getName(), new Channel(branch));
                                    }
                                    channelLocalSnapshotMap.put(snapshotId, channelMap);
                                } else {
                                    print("channelLocalSnapshotMap contains the key " + snapshotId);
                                }


                                print("State recorded in InitSnapshot for the branch " + serverBranchPort + " state is " + localSnapshotMap.get(new Integer(snapshotId)));
                            }
                            startMarker(snapshotId);

                        } finally {
                            lock.unlock();
                        }

                        print("----Init Snapshot End----");
                    }
                    //Marker
                    else if (msg.hasMarker()) {
                        print("----Marker Start----");

                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(is));
                        String branchName = bufferedReader.readLine();
                        print(branchName);
                        Bank.Marker marker = msg.getMarker();
                        int snapshotId = marker.getSnapshotId();
                        print("Marker snapshot id " + snapshotId + " received");


                        try {
                            if (localSnapshotMap != null && !localSnapshotMap.containsKey(snapshotId)) {
                                lock.lock();
                                Bank.ReturnSnapshot.LocalSnapshot localSnapshot = Bank.ReturnSnapshot.LocalSnapshot.newBuilder()
                                        .setSnapshotId(snapshotId)
                                        .setBalance(balance)
                                        .build();//channels to be coded
                                localSnapshotMap.put(snapshotId, localSnapshot);

                                if (!channelLocalSnapshotMap.containsKey(snapshotId)) {
                                    Map<String, Channel> channelMap = new HashMap<>();
                                    for (Bank.InitBranch.Branch branch : branchMap.values()) {
                                        if (!branch.getName().equalsIgnoreCase(serverBranchName)) {
                                            channelMap.put(branch.getName(), new Channel(branch));
                                            if (branchName.equalsIgnoreCase(branch.getName())) {
                                                channelMap.get(branchName).setAcceptFlag(false);
                                            }
                                        }
                                    }
                                    print(channelMap);
                                    channelLocalSnapshotMap.put(snapshotId, channelMap);


                                } else {
                                    print("channelLocalSnapshotMap contains the key " + snapshotId);
                                }
                                print("State recorded in Marker for the branch " + serverBranchPort + " state is " + localSnapshotMap.get(new Integer(snapshotId)));

                                //code for sockets for sending the marker to n-1 channel
                                if (getMarkerFlag(snapshotId)) {
                                    startMarker(snapshotId);
                                }
                                lock.unlock();


                            } else {
                                print("Updating the channel");
                                if (channelLocalSnapshotMap != null) {
                                    lock.lock();

                                    try {
                                        Map<String, Channel> channelMap = channelLocalSnapshotMap.get(snapshotId);
                                        print("channel check for " + branchName);
                                        Channel channel = channelMap.get(branchName);
                                        channel.setAcceptFlag(false);
                                    } finally {
                                        lock.unlock();

                                    }

                                }
                                print(localSnapshotMap.get(snapshotId).getBalance());
                                print(channelLocalSnapshotMap);
                            }
                        } finally {

                        }

                        print("----Marker End----");
                    }
                    //RetrieveSnapshot called from the controller
                    else if (msg.hasRetrieveSnapshot()) {
                        print("----Retrieve Snapshot Start--");
                        int retrieveSnapshotId = msg.getRetrieveSnapshot().getSnapshotId();
                        print("RetrieveSnapshot id = " + retrieveSnapshotId);

                        if (localSnapshotMap != null && channelLocalSnapshotMap != null &&
                                localSnapshotMap.containsKey(retrieveSnapshotId) &&
                                channelLocalSnapshotMap.containsKey(retrieveSnapshotId)) {

                            Bank.ReturnSnapshot.LocalSnapshot.Builder returnLocalSnapShotBuilder = Bank.ReturnSnapshot.LocalSnapshot.newBuilder();
                            int localSnapShotBalance = localSnapshotMap.get(retrieveSnapshotId).getBalance();

                            returnLocalSnapShotBuilder.setBalance(localSnapShotBalance);//balance set
                            returnLocalSnapShotBuilder.setSnapshotId(retrieveSnapshotId);

                            lock.lock();

                            Map<String, Channel> channelMap = channelLocalSnapshotMap.get(retrieveSnapshotId);

                            TreeMap<String, Channel> treeMap = new TreeMap<>();
                            treeMap.putAll(channelMap);
                            for (String incomingBranchChannel : treeMap.keySet()) {
                                Channel channel = treeMap.get(incomingBranchChannel);
                                int sum = channel.getSumOfChannel();

                                returnLocalSnapShotBuilder.addChannelState(sum);
                            }

                            Bank.ReturnSnapshot.LocalSnapshot localSnapshot = returnLocalSnapShotBuilder.build();

                            msg = Bank.BranchMessage.newBuilder().setReturnSnapshot(Bank.ReturnSnapshot.newBuilder().setLocalSnapshot(localSnapshot)).build();

                            lock.unlock();
                            msg.writeDelimitedTo(clientSocket.getOutputStream());
                            clientSocket.shutdownOutput();

                        } else {
                            System.out.println("some thing is wrong");
                        }

                        print("----Retrieve Snapshot End----");
                        clientSocket.close();
                    }


                    print("\n\n---------------------------------------------");
                    print("=====Message received End count = " + msgCount);
                    print("---------------------------------------------\n\n");
                }
            } else {
                System.out.println("Please enter the branch name and the p");
            }
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        } catch (UnknownHostException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    private static void print(Object obj) {
        if (printFlag) {
            if (obj != null)
                System.out.println(obj.toString());
            else
                System.out.println("null");
        }
    }

    private synchronized static boolean getMarkerFlag(int snapShotId) {
        Map<String, Channel> channelMap = channelLocalSnapshotMap.get(snapShotId);
        boolean flag = false;
        for (Channel channel : channelMap.values()) {
            flag = flag || channel.isAcceptFlag();
        }
        return flag;
    }

    private synchronized static void startMarker(int snapshotId) {
        //lock.lock();
        for (String strBranch : branchMap.keySet()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Bank.Marker marker = Bank.Marker.newBuilder().setSnapshotId(snapshotId).build();

            Bank.BranchMessage msg = Bank.BranchMessage.newBuilder().setMarker(marker).build();

            Socket branchServer = null;

            if (!strBranch.equalsIgnoreCase(serverBranchName)) {
                try {
                    Bank.InitBranch.Branch branch = branchMap.get(strBranch);
                    print("Message marker sending to " + branch.getPort());

                    branchServer = new Socket(branch.getIp(), branch.getPort());

                    msg.writeDelimitedTo(branchServer.getOutputStream());

                    branchServer.getOutputStream().write(serverBranchName.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    try {
                        branchServer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        //lock.unlock();

    }


    private static Thread transferThread = new Thread(() -> {

        print("Transfer Started");

        while (true) {

            try {
                Random rn = new Random();
                int randomNumber = rn.nextInt(5) + 2;
                Thread.sleep(randomNumber * 1000);
                rn = new Random();
                randomNumber = rn.nextInt(5) + 1;
                int amountToTransfer = (initialBalance * randomNumber) / 100;
                lock.lock();
                if (balance - amountToTransfer >= 0 && branchMap.size() > 0) {

                    rn = new Random();
                    int randomBranchNumber = rn.nextInt(branchMap.size());
                    List<String> list = new ArrayList<>(branchMap.keySet());

                    String branchName = list.get(randomBranchNumber);
                    if (!branchName.equalsIgnoreCase(serverBranchName)) {
                        print("\n\n----------------------------------------");
                        print("==========Transfer started===============");
                        print("--------------------------------------------\n\n");
                        String branchIp = branchMap.get(branchName).getIp();
                        int branchPort = branchMap.get(branchName).getPort();
                        Bank.Transfer transfer = Bank.Transfer.newBuilder()
                                .setMoney(amountToTransfer)
                                .build();


                        Bank.BranchMessage msg = Bank.BranchMessage.newBuilder().setTransfer(transfer).build();

                        try {
                            balance -= amountToTransfer;
                            print("Transfer amount " + amountToTransfer + " to "
                                    + branchName
                                    + " balance " + balance);

                            Socket socket = new Socket(branchIp, branchPort);

                            msg.writeDelimitedTo(socket.getOutputStream());
                            socket.getOutputStream().write(serverBranchName.getBytes());
                            socket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        print("\n\n----------------------------------------");
                        print("==========Transfer ended===============");
                        print("--------------------------------------------\n\n");
                    }
                } else {
                    print("No Amount");
                }
                lock.unlock();

            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {

            }
        }
    });
}