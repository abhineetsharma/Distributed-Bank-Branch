import java.util.ArrayList;
import java.util.List;

public class Channel {
    private boolean acceptFlag;
    private List<Integer> incomingTransferList;
    private Bank.InitBranch.Branch branch;

    public Channel(Bank.InitBranch.Branch branchI) {
        incomingTransferList = new ArrayList<>();
        setAcceptFlag(true);
        branch = branchI;
    }


    public boolean isAcceptFlag() {
        return acceptFlag;
    }

    public synchronized void setAcceptFlag(boolean acceptFlag) {
        this.acceptFlag = acceptFlag;
    }

    public synchronized void add(int amount) {
        if (isAcceptFlag())
            incomingTransferList.add(amount);
    }

    public List<Integer> getincomingTransferList() {
        return incomingTransferList;
    }

    public String toString() {
        return " " + isAcceptFlag() + " " + incomingTransferList.toString() + " ";
    }


    public Bank.InitBranch.Branch getBranch() {
        return branch;
    }

    public synchronized int getSumOfChannel() {
        int sum = 0;
        for (int s : incomingTransferList)
            sum += s;
        return sum;
    }

}
