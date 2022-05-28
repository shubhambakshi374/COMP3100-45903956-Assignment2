import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
// import java.util.ArrayList;
import java.util.HashMap;
// import java.util.HashSet;
import java.util.List;
// import java.util.Set;
// import java.util.stream.Collectors;

/**
 * TCPClient
 */
public class Assignment2 {

  static Integer NOS;
  static HashMap<Integer, List<Server>> serverMap = new HashMap<Integer, List<Server>>();
  static Socket socket = null;
  static DataOutputStream dout = null;


  public static void main(String[] args) throws Exception {
    socket = new Socket("localhost", 50000);

    dout = new DataOutputStream(socket.getOutputStream());
    String reqString = "", respString = "";

    //handshake
    String[] handShake = new String[] { "HELO", "AUTH shubham" };
    for (String hand : handShake) {
      reqString = hand;
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
    }

    while(true) {
      reqString = "REDY";
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      if(respString.contains("JOBN")) {
        Job job = parseJob(respString);
        Server server = getCapableServers(job);


        // send ok
        reqString = "OK";
        dout.write((reqString + "\n").getBytes());
        dout.flush();
        respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();

        //Schedule a job
        reqString = "SCHD " + job.getJobID() + " " + server.getServerType() + " " + server.getServerID();
        dout.write((reqString + "\n").getBytes());
        dout.flush();
        respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      } else if(respString.contains("NONE")) {
        break;
      }
    }

    reqString = "QUIT\n";
    dout.write(reqString.getBytes());
    dout.flush();
    dout.close();
    socket.close();
  }

  public static Server getCapableServers(Job job) throws IOException {
    String reqString = "GETS Capable " + String.valueOf(job.getJobCore()) + " " + String.valueOf(job.getJobMem()) + " " + String.valueOf(job.getJobDisk());
    dout.write((reqString + "\n").getBytes());
    dout.flush();
    String respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
    NOS = Integer.parseInt(respString.split(" ")[1]);
    if(NOS > 0) {
      reqString = "OK";
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      // System.out.println(reader.readLine());
      return getBestServer(reader);
    } else {
      reqString = "OK";
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      getCapableServers(job);
    }
    return null;
  }

  public static Server getBestServer(BufferedReader reader) throws IOException {
    Server server = new Server();
    for(int i=0; i < NOS; i++) {
      String line = reader.readLine();
      server = serverInfo(line);
      if(server.getServerState().equalsIgnoreCase("booting") || server.getServerState().equalsIgnoreCase("active") || server.getServerState().equalsIgnoreCase("idle")) {
        return server;
      }
    }
    return server;
  }

  public static Job parseJob(String jobString) {
    String[] jobInfo = jobString.split(" ");
    Job job = new Job();
    job.setJobCreateTime(Integer.parseInt(jobInfo[1]));
    job.setJobID(Integer.parseInt(jobInfo[2]));
    job.setJobEstimate(Integer.parseInt(jobInfo[3]));
    job.setJobCore(Integer.parseInt(jobInfo[4]));
    job.setJobMem(Integer.parseInt(jobInfo[5]));
    job.setJobDisk(Integer.parseInt(jobInfo[6]));
    return job;
  }

  public static Server serverInfo(String serverString) {
    String[] serverInfoArray = serverString.split(" ");
    Server server = new Server();
    server.setServerType(serverInfoArray[0]);
    server.setServerID(Integer.parseInt(serverInfoArray[1]));
    server.setServerState(serverInfoArray[2]);
    server.setServerStartTime(serverInfoArray[3]);
    server.setServerCore(Integer.parseInt(serverInfoArray[4]));
    server.setServerMemory(Integer.parseInt(serverInfoArray[5]));
    server.setServerWJobs(Integer.parseInt(serverInfoArray[6]));
    server.setServerRJobs(Integer.parseInt(serverInfoArray[7]));
    return server;
  }
}

class Job {

  private Integer jobCreateTime;
  private Integer jobID;
  private Integer jobEstimate;
  private Integer jobCore;
  private Integer jobMem;
  private Integer jobDisk;

  public void setJobCreateTime(Integer jobCreateTime) {
    this.jobCreateTime = jobCreateTime;
  }

  public void setJobID(Integer jobID) {
    this.jobID = jobID;
  }

  public void setJobEstimate(Integer jobEstimate) {
    this.jobEstimate = jobEstimate;
  }

  public void setJobCore(Integer jobCore) {
    this.jobCore = jobCore;
  }

  public void setJobMem(Integer jobMem) {
    this.jobMem = jobMem;
  }

  public void setJobDisk(Integer jobDisk) {
    this.jobDisk = jobDisk;
  }

  public Integer getJobCreateTime() {
    return jobCreateTime;
  }

  public Integer getJobID() {
    return jobID;
  }

  public Integer getJobEstimate() {
    return jobEstimate;
  }

  public Integer getJobCore() {
    return jobCore;
  }

  public Integer getJobMem() {
    return jobMem;
  }

  public Integer getJobDisk() {
    return jobDisk;
  }
}


class Server {

  private String serverType;
  private int serverID;
  private String serverState;
  private String serverStartTime;
  private int serverCore;
  private int serverMemory;
  private int serverDisk;
  private int serverWJobs;
  private int serverRJobs;

  public void setServerType(String serverType) {
    this.serverType = serverType;
  }

  public void setServerID(int serverID) {
    this.serverID = serverID;
  }

  public void setServerState(String serverState) {
    this.serverState = serverState;
  }

  public void setServerStartTime(String serverStartTime) {
    this.serverStartTime = serverStartTime;
  }

  public void setServerCore(int serverCore) {
    this.serverCore = serverCore;
  }

  public void setServerMemory(int serverMemory) {
    this.serverMemory = serverMemory;
  }

  public void setServerDisk(int serverDisk) {
    this.serverDisk = serverDisk;
  }

  public void setServerWJobs(int serverWJobs) {
    this.serverWJobs = serverWJobs;
  }

  public void setServerRJobs(int serverRJobs) {
    this.serverRJobs = serverRJobs;
  }

  public String getServerType() {
    return serverType;
  }

  public int getServerID() {
    return serverID;
  }

  public String getServerState() {
    return serverState;
  }

  public String getServerStartTime() {
    return serverStartTime;
  }

  public int getServerCore() {
    return serverCore;
  }

  public int getServerMemory() {
    return serverMemory;
  }

  public int getServerDisk() {
    return serverDisk;
  }

  public int getServerWJobs() {
    return serverWJobs;
  }

  public int getServerRJobs() {
    return serverRJobs;
  }

  public String toString() {
    return (
      "Server Type: " +
      serverType +
      " Server ID: " +
      String.valueOf(serverID) +
      " Server State: " +
      serverState +
      " Server Start Time: " +
      serverStartTime +
      " Server Core: " +
      String.valueOf(serverCore) +
      " Server Memory: " +
      String.valueOf(serverMemory) +
      " Server Disk: " +
      String.valueOf(serverDisk) +
      " Server With Jobs: " +
      String.valueOf(serverWJobs) +
      " Server Running Jobs: " +
      String.valueOf(serverRJobs)
    );
  }
}
