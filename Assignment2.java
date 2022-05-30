import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * TCPClient
 */
public class Assignment2 {

  static Integer NOS;
  static HashMap<Integer, List<Server>> serverMap = new HashMap<Integer, List<Server>>();
  static Socket socket = null;
  static DataOutputStream dout = null;
  static List<Server> allServers = new ArrayList<Server>();
  static HashMap<Job, Server> jobMap = new HashMap<Job, Server>();
  static List<Job> allJobs = new ArrayList<Job>();

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

    reqString = "REDY";
    dout.write((reqString + "\n").getBytes());
    dout.flush();
    respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();

    if(respString.contains("JOBN")) {
      Job job = parseJob(respString);
      allJobs.add(job);
      reqString = "GETS All";
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      NOS = Integer.parseInt(respString.split(" ")[1]);

      // send ok
      reqString = "OK";
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      addAllServers(reader);
      
      // ack
      reqString = "OK";
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      
      Server server = getAvailableServer(job);
      jobMap.put(job, server);
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
    }
    while(true) {
      reqString = "REDY";
      dout.write((reqString + "\n").getBytes());
      dout.flush();
      respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      if(respString.contains("JOBN")) {
        Job job = parseJob(respString);
        allJobs.add(job);
       
        Server server = getAvailableServer(job);
        if(server == null) {
          server = getCapableServer(job);
        }
        jobMap.put(job, server);
        //Schedule a job
        reqString = "SCHD " + job.getJobID() + " " + server.getServerType() + " " + server.getServerID();
        dout.write((reqString + "\n").getBytes());
        dout.flush();
        respString = new BufferedReader(new InputStreamReader(socket.getInputStream())).readLine();
      } else if(respString.contains("NONE")) {
        break;
      } else if(respString.contains("JCPL")) {
        updateServer(respString.split(" ")[2]);
      }
    }

    reqString = "QUIT\n";
    dout.write(reqString.getBytes());
    dout.flush();
    dout.close();
    socket.close();
  }

  public static void addAllServers(BufferedReader reader) throws IOException {
    for(int i = 0; i < NOS; i++) {
      String serverLine = reader.readLine();
      Server server = serverInfo(serverLine);
      allServers.add(server);
    }
  }

  // que in server with sufficient cores
  public static Server getAvailableServer(Job job) {
    Server server = new Server();
    int index = 0;
    boolean flag = false;
    for(int i = 0; i < allServers.size(); i++) {
      if(allServers.get(i).getAvailableCores() >= job.getJobCore()) {
        if(allServers.get(i).getServerState().equalsIgnoreCase("inactive") && !flag) {
          server = allServers.get(i);
          flag = true;
        } else if(allServers.get(i).getServerState().equals("active")) {
          server = allServers.get(i);
          server.setAvailableCores(server.getAvailableCores() - job.getJobCore());
          allServers.set(i, server);
          return server;
        }
      }
    }
    if(flag) {
      server.setAvailableCores(server.getAvailableCores() - job.getJobCore());
      server.setServerState("active");
      allServers.set(index, server);
      return server;
    } else {
      return null;
    } 
  }

  // in case no sufficient cores found
  public static Server getCapableServer(Job job) {
    Server server = new Server();
    int index = 0;
    for(int i = 0; i < allServers.size(); i++) {
      if(job.getJobCore() <= allServers.get(i).getServerCore()) {
        if(allServers.get(i).getServerState().equals("inactive")) {
          server = allServers.get(i);
          index = i;
        } else if(allServers.get(i).getServerState().equals("active")) {
          server = allServers.get(i);
          return server;
        }
      }
    }
    server.setServerState("active");
    allServers.set(index, server);
    return server;
  }

  // whem jcpl recieved
  public static void updateServer(String jobId) {
    Job rJob = new Job();
    for(Job job: allJobs) {
      if(job.getJobID() == Integer.parseInt(jobId)) {
        rJob = job;
        break;
      }
    }
    Server server = jobMap.get(rJob);
    for(int i = 0; i < allServers.size(); i++) {
      if(server.getServerType().equals(allServers.get(i).getServerType()) && server.getServerID() == allServers.get(i).getServerID()) {
        server.setAvailableCores(server.getAvailableCores() + rJob.getJobCore());
        allServers.set(i, server);
        break;
      }
    }
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
    System.out.println(serverString);
    String[] serverInfoArray = serverString.split(" ");
    Server server = new Server();
    server.setServerType(serverInfoArray[0]);
    server.setServerID(Integer.parseInt(serverInfoArray[1]));
    server.setServerState(serverInfoArray[2]);
    server.setServerStartTime(serverInfoArray[3]);
    server.setServerCore(Integer.parseInt(serverInfoArray[4]));
    server.setAvailableCores(Integer.parseInt(serverInfoArray[4]));
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
  private Integer serverID;
  private String serverState;
  private String serverStartTime;
  private Integer serverCore;
  private Integer serverMemory;
  private Integer serverDisk;
  private Integer serverWJobs;
  private Integer serverRJobs;
  private Integer availableCores;

  public void setServerType(String serverType) {
    this.serverType = serverType;
  }

  public void setServerID(Integer serverID) {
    this.serverID = serverID;
  }

  public void setServerState(String serverState) {
    this.serverState = serverState;
  }

  public void setServerStartTime(String serverStartTime) {
    this.serverStartTime = serverStartTime;
  }

  public void setServerCore(Integer serverCore) {
    this.serverCore = serverCore;
  }

  public void setServerMemory(Integer serverMemory) {
    this.serverMemory = serverMemory;
  }

  public void setServerDisk(Integer serverDisk) {
    this.serverDisk = serverDisk;
  }

  public void setServerWJobs(Integer serverWJobs) {
    this.serverWJobs = serverWJobs;
  }

  public void setServerRJobs(Integer serverRJobs) {
    this.serverRJobs = serverRJobs;
  }

  public void setAvailableCores(Integer availableCores) {
    this.availableCores = availableCores;
  }

  public String getServerType() {
    return serverType;
  }

  public Integer getServerID() {
    return serverID;
  }

  public String getServerState() {
    return serverState;
  }

  public String getServerStartTime() {
    return serverStartTime;
  }

  public Integer getServerCore() {
    return serverCore;
  }

  public Integer getServerMemory() {
    return serverMemory;
  }

  public Integer getServerDisk() {
    return serverDisk;
  }

  public Integer getServerWJobs() {
    return serverWJobs;
  }

  public Integer getServerRJobs() {
    return serverRJobs;
  }

  public Integer getAvailableCores() {
    return availableCores;
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
