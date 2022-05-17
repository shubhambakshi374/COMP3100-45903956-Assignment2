import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * TCPClient
 */
public class Assignment2 {

  public static void main(String[] args) throws Exception {
    Socket socket = new Socket("localhost", 50000);

    DataOutputStream dout = new DataOutputStream(socket.getOutputStream());

    int jobSubmitted = 0;
    String reqString = "", respString = "";

    //handshake
    String[] handShake = new String[] { "HELO", "AUTH shubham" };
    for (String hand : handShake) {
      reqString = hand;
      System.out.println("Client Says: " + reqString);
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      System.out.println("Server says: " + respString);
    }
    //scheduling jobs
    while (true) {
      reqString = "REDY\n";
      dout.write(reqString.getBytes());
      dout.flush();
      respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      Job job = parseJob(respString);
      reqString = "GETS Capable " + String.valueOf(job.getJobCore()) + " " + String.valueOf(job.getJobMem()) + " "  + String.valueOf(job.getJobDisk());
      System.out.println("Client Says: " + reqString);
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      System.out.println("Server Says: " + respString);
      reqString = "OK\n";
      System.out.println("Client Says: " + reqString.getBytes());
      dout.write(reqString.getBytes());
      dout.flush();
      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      List<Server> capableServers = serverSegregation(reader);
      Server nextServer = getBestServer(capableServers);
      if(nextServer == null) {

      }

      if (respString.equals("NONE")) {
        break;
      }
    }

    socket.close();
  }

  public static List<Server> serverSegregation(BufferedReader reader) throws IOException {
    List<Server> capableServers = new ArrayList<Server>();
    while(reader.ready()) {
      String server = reader.readLine();
      capableServers.add(serverInfo(server));
    }
    return capableServers;
  }

  public static Server getBestServer(List<Server> servers) {
    boolean idleFound = false;
    Server bestServer = new Server();
      for(Server server: servers) {
        if(server.getServerState().equalsIgnoreCase("inactive")) {
          bestServer = server;
          break;
        } else if(server.getServerState().equalsIgnoreCase("active") && !idleFound) {
          bestServer = server;
        } else if(server.getServerState().equalsIgnoreCase("idle")) {
          bestServer = server;
          idleFound = true;
        }
      }
      return bestServer;
  }



  public static Server getCandidateServer(List<Server> capableServers, Job job) {
    Integer largeCount = 0;
    Server nextCandidate = new Server();
    for (Server server : capableServers) {
      // if(server.getServerState().equalsIgnoreCase("inactive")) {

      // }
    }
    return nextCandidate;
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
    System.out.println(serverInfoArray);
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

  public static ServerState returnServerTypeFromString(String state) {
    if (state.equalsIgnoreCase("booting")) {
      return ServerState.booting;
    } else if (state.equalsIgnoreCase("active")) {
      return ServerState.active;
    } else if (state.equalsIgnoreCase("inactive")) {
      return ServerState.inactive;
    } else if (state.equalsIgnoreCase("idle")) {
      return ServerState.idle;
    } else {
      return ServerState.unavailable;
    }
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

enum ServerState {
  booting,
  active,
  inactive,
  idle,
  unavailable,
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
